from __future__ import annotations

import cupy as cp

class Profiler:
    def __init__(self):
        self.kernels = []

    def start(self):
        cp.cuda.profiler.start()

    def stop(self):
        cp.cuda.profiler.stop()

    def get_kernels(self):
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
