import random


class ColorManager:
    def __init__(self, catched_colors: list[str]) -> None:
        self.catched_colors = catched_colors
        self.cur = 0

    def generate_colors(self, num_colors: int) -> list[str]:
        if num_colors > len(self.catched_colors):
            for _ in range(num_colors - len(self.catched_colors)):
                while True:
                    r = random.randint(0, 255)
                    g = random.randint(0, 255)
                    b = random.randint(0, 255)
                    hex_color = "#{:02x}{:02x}{:02x}".format(r, g, b)
                    if hex_color not in self.catched_colors:
                        self.catched_colors.append(hex_color)
                        break
            return self.catched_colors
        else:
            return self.catched_colors[:num_colors]

    def append_colors(self, num_colors: int) -> list[str]:
        for _ in range(num_colors):
            while True:
                r = random.randint(0, 255)
                g = random.randint(0, 255)
                b = random.randint(0, 255)
                hex_color = "#{:02x}{:02x}{:02x}".format(r, g, b)
                if hex_color not in self.catched_colors:
                    self.catched_colors.append(hex_color)
                    break
        return self.catched_colors

    def increment(self):
        if self.cur == len(self.catched_colors):
            self.generate_colors(num_colors=1)
        self.cur += 1
        return self.catched_colors[self.cur - 1]
