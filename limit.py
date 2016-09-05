class Limit:
    def __init__(self, lmin, lmax):
        self.min = lmin
        self.max = lmax
        self.current = lmax

        self.bound_top = lmax
        self.bound_bottom = lmin

    def get(self):
        return self.current

    def reset(self, cur):
        self.current = self.max

    def increase(self):
        self.current += self.max / 40
        if self.current > self.max:
            self.current = self.max

    def decrease(self):
        self.current -= self.max / 10
        if self.current < self.min:
            self.current = self.min

