"""Module for TTL status management."""

# pylint: disable=import-error, no-name-in-module
import asyncio
import dataclasses
import enum
import time
from typing import Any
from pprint import pprint

from artiq.coredevice.comm_moninj import CommMonInj, TTLOverride, TTLProbe
from lolenc.device.moninj import LolencCommMonInj

from protocols import SortedQueue

class DeviceChannelMapping:
    """Maps TTL devices and channels."""
    control_system = "artiq"

    def __init__(self, ttl_devices: list[str], device_db: dict[str, Any]):
        """

        When control_system is "artiq":
            channel = TTL Channel Number

        When control_system is "lolenc":
            channel = TTL_Controller AXI Channel|(TTL Device Index << 6)

        Args:
            ttl_devices: See main.configs.
            device_db: See main.device_db.
        """
        self._device_to_channel = {}
        self._channel_to_device = {}
        if self.control_system == "artiq":
            for device in ttl_devices:
                channel = device_db[device]["arguments"]["channel"]
                self._device_to_channel[device] = channel
                self._channel_to_device[channel] = device
        else:
            device_db_ttl_ctrl = [
                device for device in device_db
                if device_db[device]["class"] == "TTL_Controller"
            ]

            print("TTL Controller List : ")
            print(device_db_ttl_ctrl)

            for ttl_controller in device_db_ttl_ctrl:
                for i, ttl_dev in enumerate(device_db[ttl_controller]["arguments"]["ttl_device"]):
                    channel = device_db[ttl_controller]["arguments"]["channel"] | (i << 6)
                    self._device_to_channel[ttl_dev] = channel
                    self._channel_to_device[channel] = ttl_dev

            print("Device to Channel List : ")
            pprint(self._device_to_channel)

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

    def __init__(
            self,
            device_channel_mapping: DeviceChannelMapping,
            control_system : str = "artiq"
        ):
        """
        Args:
            device_channel_mapping: Provides the mapping of TTL devices and channels.
        """
        self._device_channel_mapping = device_channel_mapping
        self._control_system = control_system
        if control_system == "lolenc":
            self.connection = LolencCommMonInj(self.monitor_cb, self.injection_status_cb)
        else:
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
            channel = self._device_channel_mapping.channel(device)
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
            channel = self._device_channel_mapping.channel(device)
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
        for ty in set(modification_types):
            device = self._device_channel_mapping.device(ty.channel)
            if device not in devices:
                continue
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
