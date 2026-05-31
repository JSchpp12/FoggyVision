import os
import cv2
import sqlite3
import numpy as np
import json
from matplotlib import pyplot as plt
from pathlib import Path
from fogvis.db import ImageEntity, Database
from scipy.stats import norm 
from dataclasses import dataclass, asdict

def read_color_to_grey(path : Path): 
    img = cv2.imread(path)
    if img is None:
        raise Exception(f"Failed to read image {path}")
    
    grey = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
    return grey

def calculate_average_grey_image(image_paths : list[Path]) -> np.ndarray:
    first = read_color_to_grey(image_paths[0])
    
    if first is None:
        raise Exception("Failed to read first image")
    
    h,w = first.shape
    avg_image = np.zeros((h,w), dtype=np.float32)

    for path in image_paths:
        img = read_color_to_grey(path)

        if img.shape != (h,w): 
            raise ValueError(f"Image {path} has different size than expected")
        
        avg_image += img.astype(np.float32)

    avg_image /= len(image_paths)
    avg_image_uint8 = avg_image.astype(np.uint8)

    return avg_image_uint8

def calculate_grey_normal_distribution(image : np.ndarray, mask=None): 
    pixels = image.ravel()
    if mask is not None:
        pixels = pixels[mask]

    mean = np.mean(pixels)
    std_dev = np.std(pixels)

    return (mean, std_dev)