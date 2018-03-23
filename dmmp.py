"""
Python API for multipath-tools
"""
# Copyright (C) 2016 Red Hat, Inc.
# This library is free software; you can redistribute it and/or
# modify it under the terms of the GNU Lesser General Public
# License as published by the Free Software Foundation; either
# version 3 of the License, or (at your option) any later version.
#
# This library is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
# Lesser General Public License for more details.
#
# You should have received a copy of the GNU Lesser General Public
# License along with this library; If not, see <http://www.gnu.org/licenses/>.
#
# Author: Gris Ge <fge@redhat.com>

import json
import socket
import ctypes
import sys
import struct


_API_VERSION_MAJOR = 0

_IPC_ADDR = "\0/org/kernel/linux/storage/multipathd"

_IPC_LEN_SIZE = ctypes.sizeof(ctypes.c_ssize_t(0))


def _len_to_ssize_t_bytes(len_value):
    try:
        return struct.pack("n", len_value)
    except struct.error:
        h = "%x" % len_value
        s = ("0" * (len(h) % 2) + h).zfill(_IPC_LEN_SIZE * 2).decode("hex")
        if sys.byteorder == "little":
            s = s[::-1]
        return bytearray(s)


def _bytes_to_len(len_bytes):
    try:
        return struct.unpack("n", len_bytes)[0]
    except struct.error:
        if sys.byteorder == "little":
            len_bytes = len_bytes[::-1]
        return int(len_bytes.encode("hex"), 16)


def _add_reverse_mapping(d):
    for k, v in d.items():
        d[v] = k


class Path(object):
    """
    Path is the abstraction of path in multipath-tools.
    """
    def __init__(self, path):
        """
        Internal function. For mpaths_get() only.
        """
        self._dev = path["dev"]
        self._status = self._status_str_to_enum(path["chk_st"])

    STATUS_UNKNOWN = 0
    STATUS_DOWN = 2
    STATUS_UP = 3
    STATUS_SHAKY = 4
    STATUS_GHOST = 5
    STATUS_PENDING = 6
    STATUS_TIMEOUT = 7
    STATUS_DELAYED = 9

    _STATUS_CONV = {
        STATUS_UNKNOWN: "undef",
        STATUS_UP: "ready",
        STATUS_DOWN: "faulty",
        STATUS_SHAKY: "shaky",
        STATUS_GHOST: "ghost",
        STATUS_PENDING: "i/o pending",
        STATUS_TIMEOUT: "i/o timeout",
        STATUS_DELAYED: "delayed",
    }

    _add_reverse_mapping(_STATUS_CONV)

    def _status_str_to_enum(self, status_str):
        return self._STATUS_CONV.get(status_str, self.STATUS_UNKNOWN)

    @property
    def blk_name(self):
        """
        String.  Block name of current path. Examples: "sda", "nvme0n1".
        """
        return self._dev

    @property
    def status(self):
        """
        Integer. Status of current path. Possible values are:
        * Path.STATUS_UNKNOWN
            Unknown status.
        * Path.STATUS_DOWN
            Path is down and you shouldn't try to send commands to it.
        * Path.STATUS_UP
            Path is up and I/O can be sent to it.
        * Path.STATUS_SHAKY
            Only emc_clariion checker when path not available for "normal"
            operations.
        * Path.STATUS_GHOST
            Only hp_sw and rdac checkers.  Indicates a "passive/standby" path
            on active/passive HP arrays. These paths will return valid answers
            to certain SCSI commands (tur, read_capacity, inquiry, start_stop),
            but will fail I/O commands.
            The path needs an initialization command to be sent to it in order
            for I/Os to succeed.
        * Path.STATUS_PENDING
            Available for all async checkers when a check IO is in flight.
        * Path.STATUS_TIMEOUT
            Only tur checker when command timed out.
        * Path.STATUS_DELAYED
            If a path fails after being up for less than delay_watch_checks
            checks, when it comes back up again, it will not be marked as up
            until it has been up for delay_wait_checks checks. During this
            time, it is marked as "delayed".
        """
        return self._status

    @property
    def status_string(self):
        """
        String. Status of current path. Possible values are:
        * "undef"
            Path.STATUS_UNKNOWN
        * "faulty"
            Path.STATUS_DOWN
        * "ready"
            Path.STATUS_UP
        * "shaky"
            Path.STATUS_SHAKY
        * "ghost"
            Path.STATUS_GHOST
        * "i/o pending"
            Path.STATUS_PENDING
        * "i/o timeout"
            Path.STATUS_TIMEOUT
        * "delayed"
            Path.STATUS_DELAYED
        """
        return self._STATUS_CONV[self._status]

    def __str__(self):
        return "%s|%s" % (self.blk_name, self.status_string)


class PathGroup(object):
    """
    PathGroup is the abstraction of path group in multipath-tools.
    """
    def __init__(self, pg):
        """
        Internal function. For mpaths_get() only.
        """
        self._paths = [Path(path) for path in pg["paths"]]
        self._group = pg["group"]
        self._pri = pg["pri"]
        self._selector = pg["selector"]
        self._status = self._status_str_to_enum(pg["dm_st"])

    STATUS_UNKNOWN = 0
    STATUS_ENABLED = 1
    STATUS_DISABLED = 2
    STATUS_ACTIVE = 3

    _STATUS_CONV = {
        STATUS_UNKNOWN: "undef",
        STATUS_ENABLED: "enabled",
        STATUS_DISABLED: "disabled",
        STATUS_ACTIVE: "active",
    }

    _add_reverse_mapping(_STATUS_CONV)

    def _status_str_to_enum(self, status_str):
        return self._STATUS_CONV.get(status_str, self.STATUS_UNKNOWN)

    @property
    def id(self):
        """
        Integer. Group ID of current path group. Could be used for
        switching active path group.
        """
        return self._group

    @property
    def status(self):
        """
        Integer. Status of current path group. Possible values are:
        * PathGroup.STATUS_UNKNOWN
            Unknown status
        * PathGroup.STATUS_ENABLED
            Standby to be active
        * PathGroup.STATUS_DISABLED
            Disabled due to all path down
        * PathGroup.STATUS_ACTIVE
            Selected to handle I/O
        """
        return self._status

    @property
    def status_string(self):
        """
        String. Status of current path group. Possible values are:
        * "undef"
            PathGroup.STATUS_UNKNOWN
        * "enabled"
            PathGroup.STATUS_ENABLED
        * "disabled"
            PathGroup.STATUS_DISABLED
        * "active"
            PathGroup.STATUS_ACTIVE
        """
        return self._STATUS_CONV[self._status]

    @property
    def priority(self):
        """
        Integer. Priority of current path group. The enabled path group with
        highest priority will be next active path group if active path group
        down.
        """
        return self._pri

    @property
    def selector(self):
        """
        String. Selector of current path group. Path group selector determines
        which path in active path group will be use to next I/O.
        """
        return self._selector

    @property
    def paths(self):
        """
        List of Path objects.
        """
        return self._paths

    def __str__(self):
        return "%s|%s|%d" % (
            self.id, self.status_string, self.priority)


class MPath(object):
    """
    MPath is the abstraction of mpath(aka. map) in multipath-tools.
    """
    def __init__(self, mpath):
        """
        Internal function. For mpaths_get() only.
        """
        self._path_groups = [PathGroup(pg) for pg in mpath["path_groups"]]
        self._uuid = mpath["uuid"]
        self._name = mpath["name"]
        self._sysfs = mpath["sysfs"]

    @property
    def wwid(self):
        """
        String. WWID of current mpath.
        """
        return self._uuid

    @property
    def name(self):
        """
        String. Name(alias) of current mpath.
        """
        return self._name

    @property
    def path_groups(self):
        """
        List of MPath objects.
        """
        return self._path_groups

    @property
    def paths(self):
        """
        List of Path objects
        """
        rc = []
        for pg in self.path_groups:
            rc.extend(pg.paths)
        return rc

    @property
    def kdev_name(self):
        """
        The string for DEVNAME used by kernel in uevent.
        """
        return self._sysfs

    def __str__(self):
        return "'%s'|'%s'" % (self.wwid, self.name)


def _ipc_exec(s, cmd):
    buff = _len_to_ssize_t_bytes(len(cmd) + 1) + bytearray(cmd, 'utf-8') + \
        b'\0'
    s.sendall(buff)
    buff = s.recv(_IPC_LEN_SIZE)
    if not buff:
        return ""
    output_len = _bytes_to_len(buff)
    output = s.recv(output_len).decode("utf-8")
    return output.strip('\x00')


def mpaths_get():
    """
    Usage:
        Query all multipath devices.
    Parameters:
        void
    Returns:
        MPath, ...     Iterable of MPath objects.
    """
    s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    s.settimeout(60)
    s.connect(_IPC_ADDR)
    json_str = _ipc_exec(s, "show maps json")
    s.close()
    if len(json_str) == 0:
        return
    all_data = json.loads(json_str)
    if all_data["major_version"] != _API_VERSION_MAJOR:
        raise exception("incorrect version")

    for mpath in all_data["maps"]:
        yield MPath(mpath)
