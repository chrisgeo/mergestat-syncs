import json
import mimetypes
import os
from pathlib import Path
import asyncio
from git import Repo
import aiopg

# === CONFIGURATION ===
REPO_PATH = "<repo path>"
# generic pg string
DB_CONN_STRING = (
    "postgres://postgres:password@localhost:5432/postgres"
    "?sslmode=disable"
)
BATCH_SIZE = 10  # Insert every n files
REPO_UUID = "<merge stat repo id>"
# GIT_FILES = "./<your file>.json" # alternatively git ls-files


async def connect_db():
    return await aiopg.connect(DB_CONN_STRING)


def get_files():
    if os.path.exists(GIT_FILES):
        with open(GIT_FILES, "r", encoding="utf-8") as f:
            return [file["path"] for file in json.load(f)]
    else:
        print(f"File {GIT_FILES} not found.")
        return []


async def parse_blame(repo, filepath):
    """
    Parse git blame information for a given file and return a list of blame data tuples.

    :param repo: The git repository object.
    :param filepath: The path to the file for which blame information is to be parsed.
    :return: A list of tuples, each containing blame data:
        (repo_uuid, author_email, author_name, author_when, commit_hash, line_no, line, path)
    """

    blame_data = []
    rel_path = os.path.relpath(filepath, REPO_PATH)
    try:
        blame_info = repo.blame("HEAD", rel_path)
        line_no = 1
        for commit, lines in blame_info:
            for line in lines:
                blame_data.append((
                    REPO_UUID,
                    commit.author.email,
                    commit.author.name,
                    commit.committed_datetime,
                    commit.hexsha,
                    line_no,
                    line.rstrip("\n"),
                    rel_path,
                ))
                line_no += 1
    except Exception as e:
        print(f"Error processing {rel_path}: {e}")
    return blame_data


async def insert_blame_data(conn, data_batch):
    """
    Insert a batch of blame data into the database.

    :param cursor: Open database cursor
    :param data_batch: List of tuples, each containing blame data:
        (repo_id, author_email, author_name, author_when, commit_hash, line_no, line, path)
    """
    if not data_batch:
        return
    async with conn.cursor() as cursor:
        args_str = ",".join(
            cursor.mogrify("(%s,%s,%s,%s,%s,%s,%s,%s)", row).decode()
            for row in data_batch
        )
        query = (
            """
            INSERT INTO git_blame (
                repo_id,
                author_email,
                author_name,
                author_when,
                commit_hash,
                line_no,
                line,
                path
            ) VALUES """
            + args_str
        )
        await cursor.execute(query)


async def process_file(repo, filepath, blame_batch, conn):
    blame_data = await parse_blame(repo, filepath)
    blame_batch.extend(blame_data)
    if len(blame_batch) >= BATCH_SIZE:
        await insert_blame_data(conn, blame_batch)
        blame_batch.clear()


SKIP_EXTENSIONS = {
    ".png",
    ".jpg",
    ".jpeg",
    ".gif",
    ".bmp",
    ".pdf",
    ".ttf",
    ".otf",
    ".woff",
    ".woff2",
    ".ico",
    ".mp4",
    ".mp3",
    ".mov",
    ".avi",
    ".exe",
    ".dll",
    ".zip",
    ".tar",
    ".gz",
    ".7z",
}


def is_skippable(path):
    """
    Return True if the file is skippable (i.e. should not be processed for git blame).

    A file is considered skippable if it has a file extension that is known to be binary
    or if its mime type falls into one of the following categories:

    - image/
    - video/
    - audio/
    - application/pdf
    - font/

    Otherwise, the file is considered processable.

    :param path: The path to the file to be checked.
    :return: True if the file is skippable, False otherwise.
    """
    ext = Path(path).suffix.lower()
    if ext in SKIP_EXTENSIONS:
        return True
    mime, _ = mimetypes.guess_type(path)
    return mime and mime.startswith((
        "image/",
        "video/",
        "audio/",
        "application/pdf",
        "font/",
    ))


async def main():
    """
    Main entry point for the script.

    This script processes all files in a git repository and inserts their
    blame information into a PostgreSQL database.

    The script will skip files that are known to be binary or have a mime type
    that falls into one of the following categories:

    - image/
    - video/
    - audio/
    - application/pdf
    - font/

    The script will print the number of files found and the number of lines
    inserted into the database.

    :return: None
    """
    repo = Repo(REPO_PATH)
    # TODO insert into git_files tables
    all_files = [
        Path(REPO_PATH) / f
        for f in repo.git.ls_files().splitlines()
        if not is_skippable(f)
    ]
    if not all_files:
        print("No files to process.")
        return
    print(f"Found {len(all_files)} files to process.")
    blame_batch = []

    async with connect_db() as conn:
        tasks = [
            process_file(repo, filepath, blame_batch, conn)
            for filepath in all_files
        ]
        await asyncio.gather(*tasks)

        # Insert any remaining blame data
        if blame_batch:
            await insert_blame_data(conn, blame_batch)
            print(f"Inserted remaining {len(blame_batch)} lines.")


if __name__ == "__main__":
    asyncio.run(main())
