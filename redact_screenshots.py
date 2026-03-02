"""
Redact personal details from screenshots before committing to GitHub.

Usage:
  1. Save your raw screenshots into screenshots/raw/
  2. Run: python redact_screenshots.py
  3. Redacted images appear in screenshots/ (ready for git)

The script blurs regions containing personal info (emails, names, phone numbers).
"""
from PIL import Image, ImageDraw, ImageFilter
import os

RAW_DIR = os.path.join(os.path.dirname(__file__), "screenshots", "raw")
OUT_DIR = os.path.join(os.path.dirname(__file__), "screenshots")

# Define redaction regions per image: (x1, y1, x2, y2) in pixels
# Adjust these coordinates based on your actual screenshot dimensions.
# You can open the image in Paint to find the pixel coordinates.
REDACTIONS = {
    "01_login.png": [],  # Login page has no personal data
    "02_upload.png": [
        # Sidebar: user name + email area (adjust coords to match)
        (0, 80, 230, 210),
    ],
    "03_overview.png": [],  # Overview has no personal data
    "04_user_management.png": [
        # Email column + name column in the users table
        (60, 120, 700, 330),
        # Add user form area
        (60, 430, 700, 560),
    ],
}


def blur_region(img: Image.Image, box: tuple, intensity: int = 30) -> Image.Image:
    """Apply heavy Gaussian blur to a rectangular region."""
    region = img.crop(box)
    blurred = region.filter(ImageFilter.GaussianBlur(radius=intensity))
    img.paste(blurred, box)
    return img


def grey_region(img: Image.Image, box: tuple, color: str = "#D0D0D0") -> Image.Image:
    """Cover a rectangular region with a solid grey box."""
    draw = ImageDraw.Draw(img)
    draw.rectangle(box, fill=color)
    # Add "REDACTED" text in center
    cx = (box[0] + box[2]) // 2
    cy = (box[1] + box[3]) // 2
    draw.text((cx - 30, cy - 6), "REDACTED", fill="#888888")
    return img


def main():
    if not os.path.exists(RAW_DIR):
        os.makedirs(RAW_DIR)
        print(f"Created {RAW_DIR}/")
        print("Save your raw screenshots there and run this script again.")
        print("\nExpected files:")
        for name in REDACTIONS:
            print(f"  screenshots/raw/{name}")
        return

    processed = 0
    for filename, regions in REDACTIONS.items():
        src = os.path.join(RAW_DIR, filename)
        if not os.path.exists(src):
            print(f"  SKIP  {filename} (not found in raw/)")
            continue

        img = Image.open(src)
        for box in regions:
            img = grey_region(img, box)

        out = os.path.join(OUT_DIR, filename)
        img.save(out, quality=90, optimize=True)
        print(f"  OK    {filename} → {len(regions)} region(s) redacted")
        processed += 1

    print(f"\nDone. {processed} screenshot(s) saved to screenshots/")


if __name__ == "__main__":
    main()
