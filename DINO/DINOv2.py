# DINOv2.py
# Minimal DINOv2 embedding extractor using timm.
# Saves the CLS embedding to embedding_cls.npy and shows a histogram.

import sys
from pathlib import Path

import numpy as np
import torch
import timm
from PIL import Image
import matplotlib.pyplot as plt
from timm.data import resolve_model_data_config, create_transform

# Settings
MODEL_NAME = "vit_small_patch14_dinov2.lvd142m"  # timm model name
IMAGE_PATH = Path(__file__).parent / "sample.jpg"  # put a test image here
OUTPUT_EMB = Path(__file__).parent / "embedding_cls.npy"
DEVICE = "cuda" if torch.cuda.is_available() else "cpu"

def load_image(path: Path, transform):
    img = Image.open(path).convert("RGB")
    return transform(img).unsqueeze(0)

def main():
    if not IMAGE_PATH.exists():
        print(f"Please add an image named 'sample.jpg' at: {IMAGE_PATH}")
        sys.exit(1)

    print(f"Loading model '{MODEL_NAME}' on {DEVICE} ...")
    model = timm.create_model(MODEL_NAME, pretrained=True)
    model.eval().to(DEVICE)

    # Use model-specific transforms (handles size, crop, normalization, etc.)
    config = resolve_model_data_config(model)
    transform = create_transform(**config, is_training=False)

    x = load_image(IMAGE_PATH, transform).to(DEVICE)

    with torch.no_grad():
        feats = model.forward_features(x)
        if isinstance(feats, dict) and "x_norm_clstoken" in feats:
            cls_emb = feats["x_norm_clstoken"]          # shape [1, C]
        elif isinstance(feats, torch.Tensor) and feats.ndim == 3:
            cls_emb = feats[:, 0]                        # CLS token
        else:
            # Fallback: some timm models return pooled features from forward()
            cls_emb = model(x)

    emb_np = cls_emb.squeeze(0).detach().cpu().numpy()
    np.save(OUTPUT_EMB, emb_np)
    print(f"Saved CLS embedding -> {OUTPUT_EMB}  (dim={emb_np.shape[0]})")

    plt.figure()
    plt.title("DINOv2 Embedding Value Distribution")
    plt.hist(emb_np, bins=50)
    plt.tight_layout()
    plt.show()

if __name__ == "__main__":
    main()