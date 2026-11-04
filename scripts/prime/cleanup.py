#!/usr/bin/env python3
"""List what this stack holds on Prime: sandboxes labelled sandboxes.managed=true and the `sandboxes-ctx` images the
backend built. --delete-images deletes those images; --kill-run RUN kills the sandboxes of one run.

    python scripts/prime/cleanup.py --team TEAM_ID [--env-file .env] [--delete-images] [--kill-run RUN]
"""
import argparse
import asyncio
import os

from common import add_common_args, apply_common_args


async def main(args) -> None:
    import httpx
    from prime_sandboxes import AsyncAPIClient, AsyncImageClient
    from sandboxes.backends.prime_raw import IMAGE_NAME, PrimeRaw, get_team_id

    live = await PrimeRaw.list_live()
    print(f"{len(live)} sandbox(es) up:", [(s.id, s.state, s.labels.get("sandboxes.run")) for s in live])
    if args.kill_run:
        for sandbox in live:
            if sandbox.labels.get("sandboxes.run") == args.kill_run:
                await PrimeRaw.kill(sandbox.id)
                print("killed", sandbox.id)
    images = AsyncImageClient(AsyncAPIClient(api_key=os.environ["PRIME_API_KEY"]))
    try:
        rows = [r for r in (await images.list(team_id=get_team_id(), search=IMAGE_NAME, limit=100)).data
                if r.image_name == IMAGE_NAME]
    finally:
        await images.aclose()
    print(f"{len(rows)} {IMAGE_NAME} image(s), {sum((r.size_bytes or 0) for r in rows) / 2 ** 30:.3f} GiB")
    if not args.delete_images:
        return
    params = {"teamId": get_team_id()} if get_team_id() else None
    headers = {"Authorization": f"Bearer {os.environ['PRIME_API_KEY']}"}
    async with httpx.AsyncClient(base_url="https://api.primeintellect.ai/api/v1", headers=headers, timeout=60) as http:
        for row in rows:
            response = await http.delete(f"/images/{IMAGE_NAME}/{row.image_tag}", params=params)
            print("delete", row.display_ref, response.status_code)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    add_common_args(parser)
    parser.add_argument("--delete-images", action="store_true", help=f"delete the backend's built images")
    parser.add_argument("--kill-run", help="kill the sandboxes labelled with this run id")
    arguments = parser.parse_args()
    apply_common_args(arguments, "prime-cleanup")
    asyncio.run(main(arguments))
