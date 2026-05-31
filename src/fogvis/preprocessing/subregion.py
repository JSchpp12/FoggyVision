import cv2
import numpy as np
from typing import Optional

from dataclasses import dataclass, asdict
from pathlib import Path
from .image import calculate_average_grey_image

def select_subregions(grey_image, N, candidate_cutoff_values : list[int]):
    best_mask = None
    best_T = None

    for T in candidate_cutoff_values:
        _, mask = cv2.threshold(grey_image, T, 255, cv2.THRESH_BINARY_INV)

        #use a simple foreground vs background heuristic to check if the candidate is good
        foreground_ratio = mask.mean() / 255.0

        if 0.05 < foreground_ratio < 0.8: 
            best_mask = mask
            best_T = T

    if best_mask is None:
        raise Exception("Failed to find proper candidate")
    
    rows = np.where(best_mask.any(axis=1))[0]
    y_min, y_max = rows[0], rows[-1]

    h,w = grey_image.shape
    sub_w = w // N
    usable_w = sub_w * N

    x0 = (w - usable_w)

    subregions = []
    for i in range(N):
        x1 = x0 + sub_w
        subregions.append((x0, y_min, x1, y_max))
        x0 = x1


    return subregions, best_T


@dataclass
class SubregionRequest:
    camera_id : int
    image_paths : list[Path]
    num_regions_to_select : int
    candidate_t_values : list[int]

@dataclass
class SubregionResult: 
    camera_id : int
    subregions : Optional[list[tuple]]

def calculate_subregion_for_images(request : SubregionRequest) -> SubregionResult: 
    subregions = None

    grey_image : np.ndarray = calculate_average_grey_image(request.image_paths)
    try:
        subregions, best_t = select_subregions(grey_image, request.num_regions_to_select, request.candidate_t_values)
    except Exception:
        print(f"Failed to get subregions for camera_id: {request.camera_id}")
        subregions = None

    return SubregionResult(request.camera_id, subregions)