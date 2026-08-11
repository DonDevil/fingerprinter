"""
video_embedding_test.py

Simple proof-of-concept:
1. Builds a DINOv2 embedding database from a video (2 FPS)
2. Saves embeddings locally
3. Queries a single image against the database
4. Reports the closest matching frame and timestamp

Requirements:
pip install torch torchvision transformers pillow opencv-python numpy scipy tqdm
"""

import os
import cv2
import numpy as np
import torch
from PIL import Image
from tqdm import tqdm
from scipy.spatial.distance import cosine
from transformers import AutoImageProcessor, AutoModel

# ---------------- CONFIG ----------------

FPS_TO_SAMPLE = 2
MODEL_NAME = "facebook/dinov2-base"
DATABASE_FILE = "video_embeddings.npz"
SIMILARITY_THRESHOLD = 0.90

# ----------------------------------------

device = torch.device("cuda" if torch.cuda.is_available() else "cpu")

print(f"Using device: {device}")
print("Loading DINOv2 model...")

processor = AutoImageProcessor.from_pretrained(MODEL_NAME)
model = AutoModel.from_pretrained(MODEL_NAME).to(device)
model.eval()


def get_embedding_pil(img):
    inputs = processor(images=img, return_tensors="pt").to(device)

    with torch.no_grad():
        outputs = model(**inputs)

    emb = outputs.last_hidden_state[:, 0]
    emb = emb.cpu().numpy()[0]
    emb = emb / np.linalg.norm(emb)
    return emb


def format_time(seconds):
    h = int(seconds // 3600)
    seconds %= 3600
    m = int(seconds // 60)
    s = seconds % 60
    return f"{h:02}:{m:02}:{s:06.3f}"


def build_database(video_path):

    cap = cv2.VideoCapture(video_path)

    if not cap.isOpened():
        raise RuntimeError("Cannot open video.")

    video_fps = cap.get(cv2.CAP_PROP_FPS)

    if video_fps <= 0:
        raise RuntimeError("Cannot determine FPS.")

    total_frames = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))

    interval = max(1, int(round(video_fps / FPS_TO_SAMPLE)))

    embeddings = []
    timestamps = []
    frame_numbers = []

    total_samples = total_frames // interval + 1

    print("Building embedding database...")

    current = 0

    with tqdm(total=total_samples) as pbar:

        while True:

            ret, frame = cap.read()

            if not ret:
                break

            if current % interval == 0:

                rgb = cv2.cvtColor(frame, cv2.COLOR_BGR2RGB)
                img = Image.fromarray(rgb)

                emb = get_embedding_pil(img)

                embeddings.append(emb)
                timestamps.append(current / video_fps)
                frame_numbers.append(current)

                pbar.update(1)

            current += 1

    cap.release()

    np.savez_compressed(
        DATABASE_FILE,
        embeddings=np.array(embeddings, dtype=np.float32),
        timestamps=np.array(timestamps),
        frame_numbers=np.array(frame_numbers),
        video=os.path.abspath(video_path)
    )

    print("\nDatabase saved:", DATABASE_FILE)
    print("Frames indexed:", len(embeddings))


def load_database():

    data = np.load(DATABASE_FILE, allow_pickle=True)

    return (
        data["embeddings"],
        data["timestamps"],
        data["frame_numbers"],
        str(data["video"])
    )


def query_image(image_path):

    embeddings, timestamps, frame_numbers, video = load_database()

    img = Image.open(image_path).convert("RGB")

    query = get_embedding_pil(img)

    similarities = []

    for emb in embeddings:
        similarities.append(1 - cosine(query, emb))

    similarities = np.array(similarities)

    order = np.argsort(similarities)[::-1]

    print("\n========== TOP MATCHES ==========\n")

    for rank in range(min(10, len(order))):

        idx = order[rank]

        print(
            f"{rank+1:2d}. "
            f"Similarity: {similarities[idx]*100:6.2f}% | "
            f"Frame: {frame_numbers[idx]:7d} | "
            f"Time: {format_time(timestamps[idx])}"
        )

    best = order[0]
    best_score = similarities[best]

    print("\n================================")
    print("Video :", video)
    print("Best Similarity :", f"{best_score*100:.2f}%")
    print("Frame :", int(frame_numbers[best]))
    print("Timestamp :", format_time(float(timestamps[best])))

    if best_score >= SIMILARITY_THRESHOLD:
        print("\nRESULT : IMAGE IS LIKELY FROM THIS VIDEO")
    else:
        print("\nRESULT : IMAGE NOT FOUND IN VIDEO")

    print("================================")


def main():

    print()

    if not os.path.exists(DATABASE_FILE):

        video = input("Enter video path: ").strip()

        build_database(video)

    else:

        ans = input("Database exists. Rebuild? (y/n): ").lower()

        if ans == "y":
            video = input("Enter video path: ").strip()
            build_database(video)

    print()

    image = input("Enter query image path: ").strip()

    query_image(image)


if __name__ == "__main__":
    main()
