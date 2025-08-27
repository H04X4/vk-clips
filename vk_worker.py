import os
import aiohttp
import asyncio
import shutil
import glob
from typing import List, Dict, Tuple, Optional
from yt_dlp import YoutubeDL
from config import ACCESS_TOKEN, API_VERSION, PROCESSING_DELAY, VIDEOS_DIR, TOP_COUNT, GROUP_ID
import subprocess
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

class VKWorker:
    def __init__(self, access_token: str = ACCESS_TOKEN, api_version: str = API_VERSION, group_id: Optional[int] = GROUP_ID):
        if not access_token or not api_version:
            raise ValueError("access_token and api_version must be provided")
        self.access_token = access_token
        self.api_version = api_version
        self.group_id = group_id

    async def _api_get(self, session: aiohttp.ClientSession, method: str, params: Dict) -> Dict:
        url = f"https://api.vk.com/method/{method}"
        full = {"access_token": self.access_token, "v": self.api_version, **params}
        async with session.get(url, params=full) as resp:
            resp.raise_for_status()
            data = await resp.json()
            if "error" in data:
                raise Exception(f"VK API error: {data['error']['error_msg']}")
            return data

    async def _api_post(self, session: aiohttp.ClientSession, method: str, params: Dict) -> Dict:
        url = f"https://api.vk.com/method/{method}"
        full = {"access_token": self.access_token, "v": self.api_version, **params}
        async with session.post(url, params=full) as resp:
            resp.raise_for_status()
            data = await resp.json()
            if "error" in data:
                raise Exception(f"VK API error: {data['error']['error_msg']}")
            return data

    async def get_top_videos(self, session: aiohttp.ClientSession, count: int = TOP_COUNT) -> List[Dict]:
        try:
            data = await self._api_get(session, "shortVideo.getTopVideos", {"count": count})
            return data.get("response", {}).get("items", [])
        except Exception as e:
            logger.error(f"Error fetching top videos: {e}")
            return []

    @staticmethod
    def _ydl_download(url: str, out_dir: str) -> Optional[Tuple[str, Dict]]:
        """Synchronous download via yt-dlp; run in a separate thread via asyncio.to_thread."""
        try:
            from yt_dlp import YoutubeDL
        except ImportError:
            raise ImportError("yt-dlp is not installed. Please install it using 'pip install yt-dlp'")
        
        ydl_opts = {
            "format": "best",
            "outtmpl": os.path.join(out_dir, "%(id)s.%(ext)s"),
            "quiet": True,
            "nooverwrites": True,
        }
        try:
            with YoutubeDL(ydl_opts) as ydl:
                info = ydl.extract_info(url, download=True)
                if info.get("duration", 0) > 60:
                    logger.warning(f"Video too long: {info.get('duration')} seconds")
                    return None
                filepath = ydl.prepare_filename(info)
            files = glob.glob(os.path.splitext(filepath)[0] + ".*")
            if not files:
                return None
            return files[0], info or {}
        except Exception as e:
            logger.error(f"Download error: {e}")
            return None

    async def download_one(self, url: str, out_dir: str) -> Tuple[Optional[str], Optional[Dict], Optional[str]]:
        try:
            result = await asyncio.to_thread(self._ydl_download, url, out_dir)
            if not result:
                return None, None, "file_not_found_after_download"
            path, meta = result
            return path, meta, None
        except Exception as e:
            return None, None, str(e)

    @staticmethod
    def _uniqueize_video(path: str) -> Optional[str]:
        """Synchronous video uniqueization via ffmpeg; run in a separate thread."""
        if not shutil.which("ffmpeg"):
            raise EnvironmentError("ffmpeg is not installed or not found in PATH")
        try:
            base, ext = os.path.splitext(path)
            unique_path = f"{base}_unique{ext}"
            subprocess.run([
                'ffmpeg', '-i', path,
                '-vf', 'eq=brightness=0.005,noise=alls=1:allf=t',
                '-af', 'atempo=1.001',
                '-c:v', 'libx264', '-preset', 'fast', '-crf', '23',
                '-c:a', 'aac', '-b:a', '128k',
                '-movflags', '+faststart',
                '-y', unique_path
            ], check=True, capture_output=True)
            if os.path.exists(unique_path) and os.path.getsize(unique_path) > 0:
                os.remove(path)
                return unique_path
            return None
        except Exception as e:
            logger.error(f"Uniqueization error: {e}")
            return None

    async def uniqueize_one(self, path: str) -> Tuple[Optional[str], Optional[str]]:
        try:
            unique_path = await asyncio.to_thread(self._uniqueize_video, path)
            if not unique_path:
                return None, "uniqueization_failed"
            return unique_path, None
        except Exception as e:
            return None, str(e)

    async def short_video_create(self, session: aiohttp.ClientSession, video_path: str) -> Dict:
        file_size = os.path.getsize(video_path)
        response = await self._api_post(session, "shortVideo.create", {"file_size": str(file_size)})
        if "response" not in response or "upload_url" not in response["response"]:
            raise Exception("Failed to get upload_url from VK API")
        return response["response"]

    async def short_video_edit(self, session: aiohttp.ClientSession, video_resp: Dict, description: str) -> Dict:
        return await self._api_post(
            session, "shortVideo.edit",
            {
                "video_id": str(video_resp.get("video_id")),
                "owner_id": str(video_resp.get("owner_id")),
                "description": description,
                "privacy_view": "all",
                "can_make_duet": "1",
            }
        )

    async def short_video_publish(self, session: aiohttp.ClientSession, video_resp: Dict) -> Dict:
        return await self._api_post(
            session, "shortVideo.publish",
            {
                "video_id": str(video_resp.get("video_id")),
                "owner_id": str(video_resp.get("owner_id")),
                "license_agree": "1",
                "publish_date": "0",
                "wallpost": "0",
            }
        )

    async def short_video_create_from_group(self, session: aiohttp.ClientSession, video_path: str) -> Dict:
        if self.group_id is None:
            raise ValueError("group_id must be provided for group operations")
        file_size = os.path.getsize(video_path)
        response = await self._api_post(session, "shortVideo.create", {
            "file_size": str(file_size),
            "group_id": str(self.group_id)
        })
        if "response" not in response or "upload_url" not in response["response"]:
            raise Exception("Failed to get upload_url from VK API")
        return response["response"]

    async def short_video_edit_from_group(self, session: aiohttp.ClientSession, video_resp: Dict, description: str) -> Dict:
        if self.group_id is None:
            raise ValueError("group_id must be provided for group operations")
        return await self._api_post(session, "shortVideo.edit", {
            "video_id": str(video_resp.get("video_id")),
            "owner_id": str(video_resp.get("owner_id")),
            "group_id": str(self.group_id),
            "description": description,
            "privacy_view": "all",
            "can_make_duet": "1",
        })

    async def short_video_publish_from_group(self, session: aiohttp.ClientSession, video_resp: Dict) -> Dict:
        if self.group_id is None:
            raise ValueError("group_id must be provided for group operations")
        return await self._api_post(session, "shortVideo.publish", {
            "video_id": str(video_resp.get("video_id")),
            "owner_id": str(video_resp.get("owner_id")),
            "group_id": str(self.group_id),
            "license_agree": "1",
            "publish_date": "0",
            "wallpost": "0",
        })

    @staticmethod
    async def upload_file_to_url(session: aiohttp.ClientSession, upload_url: str, path: str) -> Dict:
        with open(path, "rb") as f:
            data = {"file": f}
            async with session.post(upload_url, data=data) as resp:
                resp.raise_for_status()
                return await resp.json()

    @staticmethod
    def build_description(item: Dict, fallback: str = "ПОДПИШИТЕСЬ НА ЛУЧШИЕ МЕМЫ") -> str:
        parts = []

        for key in ("description", "title", "caption", "text"):
            val = item.get(key)
            if isinstance(val, str) and val.strip():
                # Find all hashtags in the text
                hashtags = [word for word in val.split() if word.startswith("#")]
                parts.extend(hashtags)

   
        parts.insert(0, "ПОДПИШИТЕСЬ НА ЛУЧШИЕ МЕМЫ")

    
        owner_id = item.get("owner_id")
        vid = item.get("id")
        parts.append(f"#vkclips #top #video{owner_id}_{vid}")


        desc = " — ".join(dict.fromkeys(parts)) if parts else fallback
        return desc[:999]

    @staticmethod
    def link_from_item(item: Dict) -> str:
        return f"https://vk.com/video{item.get('owner_id')}_{item.get('id')}"

    @staticmethod
    def clean_videos_dir():
        if os.path.isdir(VIDEOS_DIR):
            if not os.access(VIDEOS_DIR, os.W_OK):
                raise PermissionError(f"No write permission for {VIDEOS_DIR}")
            shutil.rmtree(VIDEOS_DIR)
        os.makedirs(VIDEOS_DIR, exist_ok=True)

    async def run_cycle(self, progress_cb):
        state = {
            "stage": "init",
            "total": 0,
            "downloaded": 0,
            "uniqueized": 0,
            "uploaded": 0,
            "published": 0,
            "failed": 0,
            "items": [],
            "messages": [],
        }

        self.clean_videos_dir()
        state["messages"].append("🧹 Очищена папка videos/")
        await progress_cb(state)

        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=30)) as session:
            state["stage"] = "fetch_top"
            await progress_cb(state)
            items = await self.get_top_videos(session, TOP_COUNT)
            state["total"] = len(items)
            if not items:
                state["messages"].append("⚠️ Топ пуст.")
                await progress_cb(state)
                return state

            state["messages"].append(f"📥 Найдено в топе: {state['total']}")
            await progress_cb(state)

            enriched = []
            semaphore = asyncio.Semaphore(5)

            async def process_item(idx, item):
                async with semaphore:
                    link = self.link_from_item(item)
                    title = item.get("title") or item.get("description") or f"video{item.get('owner_id')}_{item.get('id')}"
                    state["messages"].append(f"⬇️ [{idx}/{state['total']}] Скачиваю: {link}")
                    await progress_cb(state)

                    path, meta, err = await self.download_one(link, VIDEOS_DIR)
                    if err or not path:
                        state["failed"] += 1
                        state["messages"].append(f"❌ Ошибка скачивания: {err or 'unknown'}")
                        enriched.append({"item": item, "link": link, "path": None, "status": "download_failed", "err": err, "title": title})
                        await progress_cb(state)
                        return
                    else:
                        state["downloaded"] += 1
                        state["messages"].append(f"✅ Скачано: {os.path.basename(path)}")
                        if meta and meta.get("title"):
                            item = {**item, "title": meta.get("title")}
                        state["messages"].append(f"🔄 Уникализация видео: {os.path.basename(path)}")
                        await progress_cb(state)
                        unique_path, unique_err = await self.uniqueize_one(path)
                        if unique_err or not unique_path:
                            state["failed"] += 1
                            state["messages"].append(f"❌ Ошибка уникализации: {unique_err or 'unknown'}")
                            enriched.append({"item": item, "link": link, "path": None, "status": "uniqueize_failed", "err": unique_err, "title": title})
                        else:
                            state["uniqueized"] += 1
                            state["messages"].append(f"✅ Уникализировано: {os.path.basename(unique_path)}")
                            enriched.append({"item": item, "link": link, "path": unique_path, "status": "uniqueized", "err": None, "title": title})
                    await progress_cb(state)

            tasks = [process_item(idx, item) for idx, item in enumerate(items, 1)]
            await asyncio.gather(*tasks)

            for idx, rec in enumerate(enriched, 1):
                if not rec["path"]:
                    continue
                path = rec["path"]
                item = rec["item"]
                desc = self.build_description(item)

                state["messages"].append(f"🚀 [{idx}/{state['total']}] Загружаю в VK: {os.path.basename(path)}")
                await progress_cb(state)

                try:
                    if self.group_id:
                        create_resp = await self.short_video_create_from_group(session, path)
                    else:
                        create_resp = await self.short_video_create(session, path)

                    upload_url = create_resp["upload_url"]
                    upload_resp = await self.upload_file_to_url(session, upload_url, path)

                    await asyncio.sleep(PROCESSING_DELAY * 2)

                    if self.group_id:
                        await self.short_video_edit_from_group(session, upload_resp, desc)
                        await self.short_video_publish_from_group(session, upload_resp)
                    else:
                        await self.short_video_edit(session, upload_resp, desc)
                        await self.short_video_publish(session, upload_resp)

                    state["uploaded"] += 1
                    state["published"] += 1
                    rec["status"] = "published"
                    state["messages"].append(f"🎉 Опубликовано: {os.path.basename(path)}")

                except Exception as e:
                    state["failed"] += 1
                    rec["status"] = "upload_failed"
                    rec["err"] = str(e)
                    state["messages"].append(f"💥 Ошибка загрузки: {e}")
                    await progress_cb(state)

        state["stage"] = "done"
        state["messages"].append("🏁 Готово.")
        await progress_cb(state)
        return state
