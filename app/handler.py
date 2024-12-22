import asyncio
import base64
from datetime import datetime, timedelta
import fnmatch
import os

from app.connection_registry import ConnectionRegistry
from app.serialiser import RedisEncoder, RedisDecoder
from app.exceptions import RedisException
from app.database import Database, RedisDBException, STREAM
from app.utils import gen_random_string


PING = "ping"
ECHO = "echo"
SET = "set"
XADD = "xadd"
GET = "get"
CONFIG = "config"
KEYS = "keys"
INFO = "info"
REPLCONF = "replconf"
PSYNC = "psync"
WAIT = "wait"
TYPE = "type"

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

        self.bytes_processed = 0

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

        return self.encoder.encode_simple_string("OK")

    def xadd(self, args):
        """
        Implement Redis XADD operation
        """
        stream_key = args[0]
        id = args[1]
        key_value_pairs = args[2:]

        try:
            stream_id = self.db.stream_add(stream_key, id, *key_value_pairs)
        except RedisDBException as exc:
            if exc.module == STREAM and exc.code == "small-top":
                raise RedisException("The ID specified in XADD is equal or smaller than the target stream top item")
            if exc.module == STREAM and exc.code == "small-first":
                raise RedisException("The ID specified in XADD must be greater than 0-0")

        return self.encoder.encode_bulk_string(stream_id)

    def get(self, key):

        key = key[0]
        value = self.db.get(key)

        return self.encoder.encode_bulk_string(value)

    def type(self, key):
        key = key[0]
        value = self.db.get(key)

        value_type = "none"

        if isinstance(value, str):
            value_type = "string"
        elif isinstance(value, dict) and value.get("type") == STREAM:
            value_type = "stream"

        return self.encoder.encode_simple_string(value_type)

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

    async def wait(self, args):
        """
        Sends response back to wait command with continuous checking for replica sync.
        Exits when either the required number of replicas are synced or timeout is reached.

        Args:
            args[0]: Required minimum number of synced replicas
            args[1]: Timeout in milliseconds
        """
        required_min_sync = int(args[0])
        timeout_ms = int(args[1])
        timeout_seconds = timeout_ms / 1000.0  # Convert milliseconds to seconds

        # Capture the current offset before sending REPLCONF
        # This ensures we wait for all writes that occurred before the WAIT command
        target_offset = self.replication_offset

        if target_offset == 0:
            return self.encoder.encode_integer(len(self.connection_registry.get_replicas()))

        # Calculate when the timeout will occur
        start_time = asyncio.get_event_loop().time()
        end_time = start_time + timeout_seconds

        # Send initial GETACK command to all replicas
        await self.write_to_replicas(self.encoder.encode_array(["REPLCONF", "GETACK", "*"]))

        while True:
            # Check current sync status
            replicas_synced = self.connection_registry.check_replica_sync(target_offset)

            # If we have enough synced replicas, return immediately
            if replicas_synced >= required_min_sync:
                return self.encoder.encode_integer(replicas_synced)

            # Check if we've exceeded the timeout
            current_time = asyncio.get_event_loop().time()
            if current_time >= end_time:
                return self.encoder.encode_integer(replicas_synced)

            # Calculate how long to wait before next check
            # Use a small interval (e.g., 100ms) but don't exceed remaining timeout
            remaining_time = end_time - current_time
            wait_time = min(0.1, remaining_time)  # 100ms check interval

            # Wait before next check
            await asyncio.sleep(wait_time)

    def replconf_getack(self, args):
        """
        Sends response back to replconf getack command.
        """

        return self.encoder.encode_array(["REPLCONF", "ACK", str(self.bytes_processed)])

    async def replconf_ack(self, args, writer):
        """
        Once master recives an acknowledgement,
        it updates the correct offset
        """

        await self.connection_registry.update_replica_offset(writer, int(args[0]))

    async def replconf(self, args, writer=None):
        """
        Sends response back to replconf command.
        """

        args = args or []

        if not args:
            raise RedisException("Currently, REPLCONF command expects subcommand")

        subcommand = args[0].lower()

        if subcommand == "getack":
            return self.replconf_getack(args[1:])

        if subcommand == "ack":
            return await self.replconf_ack(args[1:], writer)

        # If it doesn't match, send OK. We will add error handling later
        return self.encoder.encode_simple_string("OK")

    async def psync(self, args, writer):
        """
        Return fullresync response back to psync command.
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
        self.replication_offset += len(data)

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
            XADD: self.xadd,
            GET: self.get,
            CONFIG: self.config,
            KEYS: self.keys,
            INFO: self.info,
            REPLCONF: self.replconf,
            PSYNC: self.psync,
            WAIT: self.wait,
            TYPE: self.type,
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
        writer_set = {PSYNC, REPLCONF}

        if command in writer_set:
            return await kls(command_arr, writer)

        # Commands which need to be run async
        async_set = {WAIT}
        if command in async_set:
            return await kls(command_arr)

        return kls(command_arr)

    async def handle_replica(self, command_data, propogated_command):
        """
        If it is replica, it might get multiple Write commands
        """
        command_arr = RedisDecoder().multi_command_decoder(command_data)

        # Commands to which Replicas need to reply in case of propogation
        reply_back_commands = {REPLCONF}
        async_commands = {REPLCONF}

        responses = []

        for command, comm_length in command_arr:
            comm, comm_arr = self.get_command(command)
            kls = self.get_command_kls(comm)

            if comm in async_commands:
                response = await kls(comm_arr)
            else:
                response = kls(comm_arr)

            # Send back the response to the client
            # In case this is propogation, only send back replies when needed
            if not propogated_command or comm in reply_back_commands:
                responses.append(response)

            self.bytes_processed += comm_length

        return "".join(responses)

    async def handle(self, command_data, writer=None, propogated_command:bool=False):
        """
        Handle commands from master or replica

        Args:
            propogated_command: Is this command propogated from master to replica?
        """

        try:
            if self.is_replica:
                return await self.handle_replica(command_data, propogated_command)

            return await self.handle_master_command(command_data, writer)
        except RedisException as exc:
            return self.encoder.encode_error(f"{str(exc)}")
