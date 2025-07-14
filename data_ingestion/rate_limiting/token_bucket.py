import time

class TokenBucket:
    def __init__(self, tokens, time_unit):
        self.tokens = tokens
        self.time_unit = time_unit
        self.last_check = time.time()
        self.allowance = tokens

    def check_rate(self):
        now = time.time()
        time_passed = now - self.last_check
        self.last_check = now
        self.allowance += time_passed * (self.tokens / self.time_unit)
        if self.allowance > self.tokens:
            self.allowance = self.tokens
        if self.allowance < 1.0:
            time.sleep(1.0 - self.allowance)
            self.allowance = 0.0
        else:
            self.allowance -= 1.0
