from PIL import Image
import torch
from transformers import AutoImageProcessor, AutoModel
from scipy.spatial.distance import cosine

# -----------------------
# CONFIG
# -----------------------

IMAGE1 = "dinov2/image1.png"
IMAGE2 = "dinov2/image2.png"

MODEL_NAME = "facebook/dinov2-base"

# -----------------------
# Load Model
# -----------------------

print("Loading model...")

processor = AutoImageProcessor.from_pretrained(MODEL_NAME)
model = AutoModel.from_pretrained(MODEL_NAME)

device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
model.to(device)
model.eval()

print("Using:", device)

# -----------------------
# Function to generate embedding
# -----------------------

def get_embedding(image_path):

    image = Image.open(image_path).convert("RGB")

    inputs = processor(images=image, return_tensors="pt").to(device)

    with torch.no_grad():
        outputs = model(**inputs)

    # CLS token embedding
    embedding = outputs.last_hidden_state[:, 0]

    embedding = embedding.cpu().numpy()[0]

    return embedding

# -----------------------
# Generate embeddings
# -----------------------

emb1 = get_embedding(IMAGE1)
emb2 = get_embedding(IMAGE2)

# -----------------------
# Cosine Similarity
# -----------------------

similarity = 1 - cosine(emb1, emb2)

print("\nCosine Similarity:", similarity)

print("\nSimilarity Percentage: {:.2f}%".format(similarity * 100))

# -----------------------
# Human readable result
# -----------------------

if similarity > 0.95:
    print("Almost identical")

elif similarity > 0.90:
    print("Very strong match")

elif similarity > 0.80:
    print("Strong visual similarity")

elif similarity > 0.70:
    print("Moderately similar")

elif similarity > 0.50:
    print("Some similarity")

else:
    print("Different images")