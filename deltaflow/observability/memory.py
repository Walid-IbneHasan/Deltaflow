"""Resident memory of this process, from the operating system.

Stdlib only: `/proc/self/status` (VmRSS, VmHWM) on Linux, `GetProcessMemoryInfo`
(through ctypes) on Windows. Other platforms raise `OSError` rather than
guess.
"""

from __future__ import annotations

import ctypes
import sys
from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class Rss:
    #: resident set (Windows: working set) now, in bytes
    current: int
    #: highest resident set of the process's lifetime so far, in bytes
    peak: int


if sys.platform == "win32":
    from ctypes import wintypes

    class _Counters(ctypes.Structure):
        _fields_ = [
            ("cb", wintypes.DWORD),
            ("PageFaultCount", wintypes.DWORD),
            ("PeakWorkingSetSize", ctypes.c_size_t),
            ("WorkingSetSize", ctypes.c_size_t),
            ("QuotaPeakPagedPoolUsage", ctypes.c_size_t),
            ("QuotaPagedPoolUsage", ctypes.c_size_t),
            ("QuotaPeakNonPagedPoolUsage", ctypes.c_size_t),
            ("QuotaNonPagedPoolUsage", ctypes.c_size_t),
            ("PagefileUsage", ctypes.c_size_t),
            ("PeakPagefileUsage", ctypes.c_size_t),
        ]

    _psapi = ctypes.WinDLL("psapi")
    _kernel32 = ctypes.WinDLL("kernel32")
    _kernel32.GetCurrentProcess.restype = wintypes.HANDLE
    _psapi.GetProcessMemoryInfo.argtypes = [wintypes.HANDLE, ctypes.c_void_p, wintypes.DWORD]
    _psapi.GetProcessMemoryInfo.restype = wintypes.BOOL

    def rss() -> Rss:
        counters = _Counters()
        counters.cb = ctypes.sizeof(counters)
        ok = _psapi.GetProcessMemoryInfo(
            _kernel32.GetCurrentProcess(), ctypes.byref(counters), counters.cb
        )
        if not ok:
            raise OSError(ctypes.get_last_error(), "GetProcessMemoryInfo failed")
        return Rss(counters.WorkingSetSize, counters.PeakWorkingSetSize)

elif sys.platform == "linux":

    def rss() -> Rss:
        fields: dict[str, int] = {}
        with open("/proc/self/status", encoding="ascii") as status:
            for line in status:
                name, _, value = line.partition(":")
                if name in ("VmRSS", "VmHWM"):
                    fields[name] = int(value.split()[0]) * 1024  # reported in kB
        return Rss(fields["VmRSS"], fields["VmHWM"])

else:

    def rss() -> Rss:
        raise OSError(f"resident memory measurement is not implemented on {sys.platform}")


def describe(n: int) -> str:
    return f"{n / (1 << 20):.1f} MiB"


__all__ = ["Rss", "describe", "rss"]
