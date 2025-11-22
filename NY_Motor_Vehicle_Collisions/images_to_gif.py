import imageio.v2 as imageio
import os
import numpy as np
from PIL import Image, ImageDraw, ImageFont

png_folder = r"C:\Users\jwens\Desktop\PostGrad-Projects\NY_Motor_Vehicle_Collisions\Analytics\by_hour"
os.chdir(png_folder)

def hour_to_ampm(hour):
    if hour == 0:
        return "12 AM"
    elif hour < 12:
        return f"{hour} AM"
    elif hour == 12:
        return "12 PM"
    else:
        return f"{hour - 12} PM"

frames = []

font_path = "arial.ttf"
font = ImageFont.truetype(font_path, 120)

for i in range(24):
    filename = f"{i:02d}.png"
    label = hour_to_ampm(i)

    img = Image.open(filename).convert("RGBA")
    draw = ImageDraw.Draw(img)

    x, y = 40, 40

    draw.text((x+4, y+4), label, font=font, fill="black")
    draw.text((x, y), label, font=font, fill="white")

    frames.append(np.array(img))

output_path = r"C:\Users\jwens\Desktop\PostGrad-Projects\NY_Motor_Vehicle_Collisions\Analytics\heatmap_by_hour.gif"

imageio.mimsave(output_path, frames, duration=600)

print("GIF saved to:", output_path)
