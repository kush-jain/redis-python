import asyncio
import base64
from datetime import datetime, timedelta
import fnmatch
import os

from app.connection_registry import ConnectionRegistry
from app.serialiser import RedisEncoder, RedisDecoder
from app.exceptions import RedisException
from app.database import Database
from app.utils import gen_random_string


PING = "ping"
ECHO = "echo"
SET = "set"
GET = "get"
CONFIG = "config"
KEYS = "keys"
INFO = "info"
REPLCONF = "replconf"
PSYNC = "psync"

EMPTY_RDB = "UkVESVMwMDEx+glyZWRpcy12ZXIFNy4yLjD6CnJlZGlzLWJpdHPAQPoFY3RpbWXCbQi8ZfoIdXNlZC1tZW3CsMQQAPoIYW9mLWJhc2XAAP/wbjv+wP9aog=="


class RedisCommandHandler:

    def __init__(self, connection_registry=None):
        self.encoder = RedisEncoder()
        self.db = Database()

        self.replication_id = None
        self.replication_offset = None

        self.connection_registry = connection_registry or ConnectionRegistry()

        self.is_replica = os.getenv("replicaof", False)
        if not self.is_replica:
            self.replication_id = gen_random_string(40)
            self.replication_offset = 0

    def ping(self, command_arr):
        return self.encoder.encode_simple_string("PONG")

    def echo(self, args):
        return self.encoder.encode_bulk_string(args[0])

    def set(self, args):
        key = args[0]
        value = args[1]

        optional_args = args[2:]
        expires_at = None

        for idx, arg in enumerate(optional_args):
            if arg.lower() == "px":
                expires_at = datetime.now() + timedelta(milliseconds=int(optional_args[idx+1]))
            elif arg.lower() == "ex":
                expires_at = datetime.now() + timedelta(seconds=int(optional_args[idx+1]))

        self.db.set(key, value, expires_at)

        if self.is_replica:
            return None

        return self.encoder.encode_simple_string("OK")

    def get(self, key):

        key = key[0]
        value = self.db.get(key)

        return self.encoder.encode_bulk_string(value)

    def keys(self, pattern):
        """
        Match the pattern against DB keys, and return that
        """

        result = []
        for key in self.db:
            if fnmatch.fnmatch(key, pattern[0]):
                result.append(key)

        return self.encoder.encode_array(result)

    def config_get(self, args):
        key = args[0]
        value = os.getenv(key)
        return self.encoder.encode_array([key, value])

    def config(self, args):

        subcommand = args[0].lower()

        config_map = {
            "get": self.config_get,
        }

        if subcommand not in config_map:
            raise RedisException(f"Invalid config subcommand: {subcommand}")

        return config_map[subcommand](args[1:])

    def info_replication(self):
        is_replica = os.getenv("replicaof")
        role = "slave" if is_replica else "master"

        # Initialize the response dictionary with the role
        response_parts = {"role": role}

        # Add additional master-specific information if the node is a master
        if role == "master":
            response_parts["master_repl_offset"] = self.replication_offset
            response_parts["master_replid"] = self.replication_id

        # Convert dictionary to formatted response lines and encode
        response_lines = [f"{key}:{value}" for key, value in response_parts.items()]
        return self.encoder.encode_bulk_string("\r\n".join(response_lines))

    def info(self, args=None):

        args = args or []

        if not args:
            raise RedisException("Currently, INFO command expects subcommand")

        subcommand = args[0].lower()

        info_map = {
            "replication": self.info_replication,
        }

        if subcommand not in info_map:
            raise RedisException(f"Invalid info subcommand: {subcommand}")

        return info_map[subcommand]()

    def replconf(self, args):
        """
        Sends response back to replconf command.
        Currently, just send back OK - we need to add actual capabilities later on
        """

        return self.encoder.encode_simple_string("OK")

    async def psync(self, args, writer):
        """
        Return fullresync response back to psync command.
        Dummy implementation for now
        """

        full_resync_command = self.encoder.encode_simple_string(
            f"FULLRESYNC {self.replication_id} {self.replication_offset}"
        ).encode('utf-8')

        empty_rdb = base64.b64decode(EMPTY_RDB)

        # Asynchronously add replica to registry
        await self.connection_registry.add_replica(
            writer,
            replication_id=self.replication_id,
            offset=self.replication_offset
        )

        return full_resync_command + self.encoder.encode_file(empty_rdb)

    async def write_to_replicas(self, data):
        """
        Write data to all registered replicas
        """
        await self.connection_registry.broadcast(data)

    def get_command(self, command_arr):
        command = command_arr
        if isinstance(command_arr, list):
            command = command_arr[0]
            command_arr = command_arr[1:]

        command = command.lower()
        return command, command_arr

    def get_command_kls(self, command):

        kls_map = {
            PING: self.ping,
            ECHO: self.echo,
            SET: self.set,
            GET: self.get,
            CONFIG: self.config,
            KEYS: self.keys,
            INFO: self.info,
            REPLCONF: self.replconf,
            PSYNC: self.psync,
        }
        kls = kls_map.get(command)
        if not kls:
            raise RedisException("Invalid command")

        return kls

    async def handle_master_command(self, command_data, writer):

        command_arr = RedisDecoder().decode(command_data)
        command, command_arr = self.get_command(command_arr)

        kls = self.get_command_kls(command)

        # Commands which need to be broadcasted to the replicas
        broadcast_set = {SET}

        if command in broadcast_set:
            await self.write_to_replicas(command_data)

        # Commands which need to be passed writer argument
        writer_set = {PSYNC}

        if command in writer_set:
            return await kls(command_arr, writer)

        return kls(command_arr)

    def handle_replica(self, command_data):
        """
        If it is replica, it might get multiple Write commands
        """
        command_arr = RedisDecoder().multi_command_decoder(command_data)

        responses = []

        for command in command_arr:
            comm, comm_arr = self.get_command(command)
            kls = self.get_command_kls(comm)
            response = kls(comm_arr)
            if response:
                responses.append(response)

        return "".join(responses)

    async def handle(self, command_data, writer=None):
        """
        Handle commands from master or replica
        """
        if self.is_replica:
            return self.handle_replica(command_data)

        return await self.handle_master_command(command_data, writer)
