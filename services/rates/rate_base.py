class RateInfoBase:
    def __init__(self, rate_name):
        self.rate_name = rate_name

    def get_rate_type(self):
        return self.rate_name

    def _hex_to_rgb(self, hex_color: str) -> tuple:
        hex_color = hex_color.lstrip("#")

        if len(hex_color) == 3:
            hex_color = hex_color * 2
        return int(hex_color[0:2], 16), int(hex_color[2:4], 16), int(hex_color[4:6], 16)
