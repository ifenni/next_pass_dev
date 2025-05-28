import colorsys
import random


def hsl_distinct_colors(n):
    colors = []
    for i in range(n):
        # Generate colors with different hues
        hue = i / float(n)
        color = colorsys.hsv_to_rgb(hue, 1.0, 1.0)
        # Convert from RGB (0-1) to hex (#RRGGBB)
        rgb = [int(c * 255) for c in color]
        hex_color = "#{:02x}{:02x}{:02x}".format(*rgb)
        colors.append(hex_color)
    return colors


def spread_rgb_colors(n):
    colors = []
    # Divide the color space into n parts
    step = 255 // n
    for i in range(n):
        # Spread out the color values across the RGB spectrum
        r = (i * step) % 256
        g = ((i + 1) * step) % 256
        b = ((i + 2) * step) % 256
        hex_color = "#{:02x}{:02x}{:02x}".format(r, g, b)
        colors.append(hex_color)
    return colors


def hsl_distinct_colors_improved(num_colors):
    colors = []
    for i in range(num_colors):
        hue = (i * 360 / num_colors) % 360
        saturation = random.randint(60, 80)
        lightness = random.randint(30, 50)
        r, g, b = colorsys.hls_to_rgb(
            hue / 360, lightness / 100, saturation / 100
            )
        hex_color = "#{:02x}{:02x}{:02x}".format(
            int(r * 255), int(g * 255), int(b * 255)
            )
        colors.append(hex_color)

    return colors
