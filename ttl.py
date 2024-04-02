"""Module for TTL status management."""

import asyncio
import dataclasses
import enum
import time
from typing import Any

from artiq.coredevice.comm_moninj import CommMonInj, TTLOverride, TTLProbe

from protocols import SortedQueue

class DeviceChannelMapping:
    """Maps TTL devices and channels."""

    def __init__(self, ttl_devices: list[str], device_db: dict[str, Any]):
        """
        Args:
            ttl_devices: See main.configs.
            device_db: See main.device_db.
        """
        self._device_to_channel = {}
        self._channel_to_device = {}
        for device in ttl_devices:
            channel = device_db[device]["arguments"]["channel"]
            self._device_to_channel[device] = channel
            self._channel_to_device[channel] = device

    def device(self, channel: int) -> str:
        """Returns the TTL device name corresponding the given TTL channel number.
        
        Args:
            channel: Target TTL channel number.
        """
        return self._channel_to_device[channel]
    
    def channel(self, device: str) -> int:
        """Returns the TTL channel number corresponding the given TTL device name.
        
        Args:
            device: Target TTL device name.
        """
        return self._device_to_channel[device]


@enum.unique
class MonitorType(enum.Enum):
    """Monitoring value type."""
    PROBE = "probe"
    LEVEL = "level"
    OVERRIDE = "override"


@dataclasses.dataclass
class StatusType:
    """Monitoring status type.
    
    Fields:
        channel: Monitoring TTL channel number.
        monitor_type: Monitoring value type.
    """
    channel: int
    monitor_type: MonitorType

    def __hash__(self) -> int:
        """Overridden."""
        return hash(f"{str(self.channel)}_{self.monitor_type.value}")


# {"MonitorType value": {"TTL device name": "modified value"}}
Modifications = dict[str, dict[str, bool]]
ModificationQueue = SortedQueue[float, StatusType]


class TTLManager:
    """Manages the connection to ARTIQ moninj proxy and TTL status.

    Attributes:
        connection: CommMonInj instance for connection to ARTIQ moninj proxy.
        queue: SortedQueue with modified StatusType.
        values: Dictionary whose keys are StatusType and values are modified values.
        modified: Event set when any value is modified.
    """

    def __init__(self):
        self.connection = CommMonInj(self.monitor_cb, self.injection_status_cb)
        self.queue = ModificationQueue()
        self.values: dict[StatusType, bool] = {}
        self.modified = asyncio.Event()

    async def connect(self, core_addr: str, ttl_devices: list[str]):
        """Connects to ARTIQ moninj proxy.
        
        Args:
            See main.configs.
        """
        await self.connection.connect(core_addr)
        for device in ttl_devices:
            channel = device_to_channel[device]
            self.connection.monitor_probe(1, channel, TTLProbe.level.value)
            self.connection.monitor_injection(1, channel, TTLOverride.level.value)
            self.connection.monitor_injection(1, channel, TTLOverride.en.value)

    def current_status(self, devices: list[str]) -> tuple[float, Modifications]:
        """Returns the current timestamp and status.
        
        Args:
            devices: List of target TTL device names.
        """
        modifications = {ty.value: {} for ty in MonitorType}
        for device in devices:
            channel = device_to_channel[device]
            for ty in MonitorType:
                modifications[ty.value][device] = self.values[StatusType(channel, ty)]
        return time.time(), modifications

    def modifications_since(
        self, devices: list[str], timestamp: float
    ) -> tuple[float, Modifications]:
        """Returns the latest timestamp and modifications since the given timestamp.
        
        Args:
            devices: List of target TTL device names.
            timestamp: Timestamp of the latest update.
        """
        modifications = {ty.value: {} for ty in MonitorType}
        latest, modification_types = self.queue.tail(timestamp)
        filtered_modification_types = filter(
            lambda ty: channel_to_device[ty.channel] in devices, set(modification_types)
        )
        for ty in filtered_modification_types:
            device = channel_to_device[ty.channel]
            modifications[ty.monitor_type.value][device] = self.values[ty]
        return latest, modifications

    def _notify_modified(self):
        """Sets and clears the modified event for the queue."""
        self.modified.set()
        self.modified.clear()

    def monitor_cb(self, channel: int, _ty: int, value: int):
        """Callback function called when any monitoring value is modified.
        
        Args:
            channel: TTL channel number.
            _ty: Type of monitoring value. See artiq.coredevice.comm_moninj.TTLProbe.
              It monitors only "TTLProbe.level", hence this is not used.
            value: Modified monitoring value.
        """
        status_type = StatusType(channel, MonitorType.PROBE)
        self.values[status_type] = bool(value)
        self.queue.push(time.time(), status_type)
        self._notify_modified()

    def injection_status_cb(self, channel: int, ty: int, value: int):
        """Callback function called when any injection status is modified.
        
        Args:
            channel: TTL channel number.
            ty: Type of injection status. See artiq.coredevice.comm_moninj.TTLOverride.
            value: Modified injection status.
        """
        monitor_type = {
            TTLOverride.level.value: MonitorType.LEVEL,
            TTLOverride.en.value: MonitorType.OVERRIDE
        }[ty]
        status_type = StatusType(channel, monitor_type)
        self.values[status_type] = bool(value)
        self.queue.push(time.time(), status_type)
        self._notify_modified()
