import os
import io
import logging
import numpy as np
import pandas as pd
import mercantile
import datashader as ds
from datashader import transfer_functions as tf
from flask import Flask, send_file, render_template
from PIL import Image, ImageDraw


app = Flask(__name__)
app.logger.setLevel(logging.DEBUG)
app.config.from_pyfile("config.py")

df = pd.read_parquet(
    app.config["DATA_FILEPATH"], engine='pyarrow')


def create_pillow_tile(x, y, zoom):
    img = Image.new('RGB', 
        size=(256, 256), 
        color=(200, 200, 200))
    draw = ImageDraw.Draw(img)
    draw.rectangle([5, 5, 250, 250], 
        width=2, 
        outline=(220, 220, 220))
    text = f"{x},{y},{zoom}"
    w, h = draw.textsize(text)
    draw.text((128 - w/2, 128 - h/2), 
        text=text, 
        fill=(150, 150, 150))

    return img


def create_datashader_tile(df, x, y, zoom):
    xy_bounds = mercantile.xy_bounds(
        mercantile.Tile(x, y, zoom))

    cvs = ds.Canvas(plot_width=256, plot_height=256,
            x_range=(xy_bounds[0], xy_bounds[2]),
            y_range=(xy_bounds[1], xy_bounds[3]))

    agg = cvs.points(df, 'x', 'y', agg=ds.count())
    img = tf.shade(agg,
        cmap=['blue', 'darkblue', 'black'],
        how='eq_hist')
    img = tf.spread(img, name="spread 1px")

    return img.to_pil()


@app.route('/')
def index():
    return render_template("index.html")


@app.route("/tiles/<int:zoom>/<int:x>/<int:y>.png")
def tile(x, y, zoom):
    if app.config["IS_CACHED"]:
        folderpath = os.path.join(
            app.config['TILES_FOLDERPATH'], 
            f"tiles/{zoom}/{x}")
        filepath = os.path.join(folderpath, f"{y}.png")

        if not os.path.exists(filepath):
            app.logger.debug(f"Create image for {filepath}")
            img = create_datashader_tile(df, x, y, zoom)
            os.makedirs(folderpath, exist_ok=True)   
            img.save(filepath, 'PNG')
    
        return send_file(filepath, mimetype='image/png')
    else:
        img = create_datashader_tile(df, x, y, zoom)
        img_bytes = io.BytesIO()
        img.save(img_bytes, 'PNG')
        img_bytes.seek(0)

    return send_file(img_bytes, mimetype='image/png')


@app.route("/pillow_tiles/<int:zoom>/<int:x>/<int:y>.png")
def pillow_tile(x, y, zoom):
    app.logger.debug(f"Create tile for /pillow_tiles/{x}/{y}/{zoom}.png")
    img = create_pillow_tile(x, y, zoom)
    img_bytes = io.BytesIO()
    img.save(img_bytes, 'PNG')
    img_bytes.seek(0)

    return send_file(img_bytes, mimetype='image/png')
