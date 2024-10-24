"""
pip install -U yt-dlp
brew upgrade && brew update ffmpeg
pip install -U certifi brotli websockets requests curl_cffi argparse mlx-whisper
"""
import glob
import json
import os
import time

import mlx_whisper
import yt_dlp
import argparse

file_parent = os.path.dirname(os.path.abspath(__file__))
audio_stem = "audio"
audio_ext = "mp3"
audio_file = f"{audio_stem}.{audio_ext}"

model_name = "mlx-community/whisper-large-v3-turbo"


def download_audio(url: str, transcript_dir: str, force: bool = False) -> str:
    info = yt_dlp.YoutubeDL().extract_info(url, download=False)
    existing_dirs = glob.glob(os.path.join(transcript_dir, f"*_{info['id']}/"))
    if existing_dirs and not force:
        return os.path.join(existing_dirs[0], audio_file)

    cleaned = info["title"].lower().replace(" ", "-").replace("/", "-")
    title_base = "".join(e for e in cleaned if e.isalnum() or e in ["-"])
    info["title"] = title_base
    dest_dir = os.path.join(transcript_dir, f"{title_base}_{info['id']}")
    options = {
        "format": "bestaudio/best",
        "outtmpl": os.path.join(dest_dir, f"{audio_stem}.%(ext)s"),
        "postprocessors": [
            {
                "key": "FFmpegExtractAudio",
                "preferredcodec": audio_ext,
                "preferredquality": "192",
            }
        ],
        "noplaylist": True,
    }
    downloader = yt_dlp.YoutubeDL(options)
    downloader.download([url])
    file_path = os.path.join(dest_dir, audio_file)
    print(f"Downloaded: {url}, saved to: {file_path}")
    return file_path


def transcribe(path: str, force: bool = False):
    raw_path = os.path.join(os.path.dirname(path), "transcript.json")
    readable_path = os.path.join(os.path.dirname(path), "transcript.md")
    done = os.path.exists(raw_path) and os.path.exists(readable_path)

    if done and not force:
        print(f"Transcript already exists for {path} - skip")
        return

    t0 = time.perf_counter()
    result = mlx_whisper.transcribe(
        path,
        path_or_hf_repo=model_name,
        initial_prompt=None,
        word_timestamps=True,
        compression_ratio_threshold=2,
        verbose=True,
    )
    t1 = time.perf_counter()
    print(f"Transcription completed in {t1 - t0:.2f} seconds.")
    final = {"text": result["text"]}
    fields = ["id", "start", "end", "text", "words"]
    final["segments"] = [
        {key: s.get(key) for key in fields} for s in result["segments"]
    ]

    with open(raw_path, "w") as f:
        f.write(json.dumps(final, indent=2))

    with open(readable_path, "w") as f:
        for segment in final["segments"]:
            f.write(f"##### [{segment['start']} --> {segment['end']}]\n\n")
            f.write(f"{segment['text']}\n\n")


def main(url: str, dest: str, keep_audio: bool = True, force: bool = False):
    fp = download_audio(url, transcript_dir=dest, force=force)
    transcribe(fp)
    if not keep_audio:
        os.remove(fp)


sample_url = "https://www.youtube.com/watch?v=DCbGM4mqEVw"

if __name__ == "__main__":
    default_dest = os.getenv("HOME") + "/Transcripts"
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", type=str, default=sample_url)  # todo required=True
    parser.add_argument("--dest", type=str, default=default_dest)
    parser.add_argument(
        "--keep",
        action="store_true",
        help="Keep audio file after processing",
        default=True,
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Force download and transcription even if transcript exists",
        default=False,
    )
    args = parser.parse_args()

    if not os.path.exists(args.dest):
        os.makedirs(args.dest)

    main(url=args.url, dest=args.dest, keep_audio=True)
