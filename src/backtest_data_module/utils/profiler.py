from __future__ import annotations

try:
    import cupy as cp
except Exception:  # noqa: BLE001
    cp = None


class Profiler:
    def __init__(self):
        self.kernels = []

    def start(self):
        if cp:
            cp.cuda.profiler.start()

    def stop(self):
        if cp:
            cp.cuda.profiler.stop()

    def get_kernels(self):
        if cp:
            self.kernels = cp.cuda.profiler.get_sorted_kernels()
        return self.kernels

    def print_report(self):
        print("--- Kernel Profile ---")
        for kernel in self.get_kernels():
            print(f"Kernel: {kernel.name}")
            print(f"  Time: {kernel.time} us")
            print(f"  Occupancy: {kernel.occupancy}")
            print(f"  Shared Memory: {kernel.shared_mem}")
        print("--------------------")
