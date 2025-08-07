import re
import os
import aiohttp
import asyncio
from bs4 import BeautifulSoup
from tqdm import tqdm
import time
import logging
from urllib.parse import urljoin
import functools # 用于 run_in_executor

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("pdb_downloader.log"),
    ]
)

class PDBDownloader:
    def __init__(self, html_file_path, output_dir, max_connections=50, retries=3, timeout=300, chunk_size=8192, max_files_per_dir=20000):
        """
        初始化PDB下载器

        :param html_file_path: 本地HTML索引文件的路径
        :param output_dir: 下载文件保存的根目录
        :param max_connections: 最大并发连接数
        :param retries: 单个文件下载失败后的重试次数
        :param timeout: 单个HTTP请求的超时时间（秒）
        :param chunk_size: 下载时每次读取的字节数
        :param max_files_per_dir: 每个子目录最多存放的文件数 (EXFAT 限制)
        """
        self.html_file_path = html_file_path
        self.output_dir = output_dir
        self.max_connections = max_connections
        self.retries = retries
        self.timeout = aiohttp.ClientTimeout(total=timeout, sock_read=timeout/2)
        self.chunk_size = chunk_size
        self.max_files_per_dir = max_files_per_dir
        self.semaphore = asyncio.Semaphore(max_connections)
        # 存储相对于 output_dir 的路径，如 "batch_00001/pdbxxxx.ent.gz"
        self.downloaded_files = set()
        self.failed_files = {}
        self.total_files_to_download = 0
        self.successfully_downloaded_count = 0
        self.download_queue = None
        self._lock = None

        os.makedirs(output_dir, exist_ok=True)
        self._load_existing_files()

    def _get_target_dir_and_path(self, filename):
        """根据文件名和当前文件计数，确定目标目录和完整路径"""
        counter = 0
        while True:
            batch_dir_name = f"batch_{counter:05d}"
            batch_dir_path = os.path.join(self.output_dir, batch_dir_name)

            if not os.path.exists(batch_dir_path):
                os.makedirs(batch_dir_path, exist_ok=True)
                logging.debug(f"Created new batch directory on demand: {batch_dir_path}")
                target_dir = batch_dir_path
                break
            else:
                try:
                    existing_files_in_batch = [f for f in os.listdir(batch_dir_path) if os.path.isfile(os.path.join(batch_dir_path, f)) and not f.endswith('.tmp')]
                    if len(existing_files_in_batch) < self.max_files_per_dir:
                        target_dir = batch_dir_path
                        break
                except OSError as e:
                    logging.error(f"Error checking batch directory {batch_dir_path}: {e}")
            counter += 1
            if counter > 100000:
                logging.critical("Too many batch directories checked. Aborting.")
                raise OSError("Could not find or create a batch directory with space.")

        filepath = os.path.join(target_dir, filename)
        # 计算相对于 output_dir 的路径，用于记录
        relative_path = os.path.relpath(filepath, self.output_dir)
        return target_dir, filepath, relative_path

    def _load_existing_files(self):
        """递归加载输出目录及其子目录中已存在的文件列表"""
        self.downloaded_files.clear()
        if os.path.exists(self.output_dir):
            for root, dirs, files in os.walk(self.output_dir):
                for file in files:
                    if not file.endswith('.tmp'):
                        full_path = os.path.join(root, file)
                        relative_path = os.path.relpath(full_path, self.output_dir)
                        self.downloaded_files.add(relative_path)
        logging.info(f"Found {len(self.downloaded_files)} existing files in output directory (including subdirs)")

    async def parse_html_producer(self):
        """解析本地HTML文件并提取下载链接 (生产者) - 异步优化版"""
        logging.info(f"Starting to parse local HTML file: {self.html_file_path}")
        pattern = re.compile(r'pdb[a-z0-9]{3,4}\.ent\.gz')
        base_url = "https://files.pdbj.org/pub/pdb/data/structures/all/pdb/"

        if not os.path.isfile(self.html_file_path):
            logging.error(f"HTML file not found: {self.html_file_path}")
            for _ in range(self.max_connections):
                await self.download_queue.put((None, None))
            return

        loop = asyncio.get_event_loop()

        try:
            logging.info("Reading HTML file content asynchronously...")
            def _read_file():
                with open(self.html_file_path, 'r', encoding='utf-8', errors='ignore') as f:
                    return f.read()
            html_content = await loop.run_in_executor(None, _read_file)
            logging.info("HTML file content read successfully.")

            logging.info("Parsing HTML content asynchronously with BeautifulSoup...")
            soup = await loop.run_in_executor(
                None,
                functools.partial(BeautifulSoup, html_content, 'lxml')
            )
            logging.info("HTML content parsed successfully.")
        except Exception as e:
            logging.error(f"Error reading or parsing local HTML file: {e}")
            for _ in range(self.max_connections):
                await self.download_queue.put((None, None))
            return

        logging.info("Extracting links from parsed HTML...")
        links_found = 0
        links_queued = 0
        for link in soup.find_all('a', href=True):
            href = link['href']
            if pattern.search(href):
                filename = href.split('/')[-1]
                links_found += 1

                # 跳过检查：基于已加载的相对路径文件名
                if any(os.path.basename(rel_path) == filename for rel_path in self.downloaded_files):
                    logging.debug(f"Skipping {filename} as it exists in downloaded_files set.")
                    continue

                full_url = urljoin(base_url, href)
                await self.download_queue.put((full_url, filename))
                links_queued += 1

        self.total_files_to_download = links_queued
        logging.info(f"Producer finished. Parsed {links_found} matching links, {links_queued} need downloading (not skipped).")

        # 发送结束信号给所有消费者
        for _ in range(self.max_connections):
            await self.download_queue.put((None, None))

    async def download_file_consumer(self, session, progress_bar):
        """从队列中获取链接并下载文件 (消费者)"""
        while True:
            url_filename_tuple = await self.download_queue.get()
            url, filename = url_filename_tuple

            # --- 关键：为整个队列项目处理逻辑包裹 try...finally ---
            # 确保无论内部循环如何退出，task_done 都只调用一次
            try:
                if url is None:
                    # 处理 None 信号 - 直接视为成功处理
                    logging.debug("Consumer received shutdown signal (None).")
                    return # 消费者协程正常退出

                # 为这个队列项目初始化变量
                success = False
                target_dir, filepath, relative_path = self._get_target_dir_and_path(filename)
                temp_filepath = filepath + '.tmp'

                # --- 精确检查文件是否存在 ---
                if relative_path in self.downloaded_files:
                    logging.debug(f"File {relative_path} already exists (precise check), skipping.")
                    async with self._lock:
                        self.successfully_downloaded_count += 1
                    success = True
                else:
                    # --- 重试循环 ---
                    for attempt in range(self.retries):
                        # 在每次尝试前重新计算路径（防御性编程）
                        target_dir, filepath, relative_path = self._get_target_dir_and_path(filename)
                        temp_filepath = filepath + '.tmp'

                        try:
                            # 再次检查，以防在等待重试时文件被其他进程/协程创建
                            if relative_path in self.downloaded_files:
                                logging.debug(f"[Attempt {attempt+1}] File {relative_path} already exists, skipping.")
                                async with self._lock:
                                    self.successfully_downloaded_count += 1
                                success = True
                                break # 成功处理，跳出重试循环

                            resume_header = {}
                            start_byte = 0
                            if os.path.exists(temp_filepath):
                                try:
                                    start_byte = os.path.getsize(temp_filepath)
                                    resume_header = {'Range': f'bytes={start_byte}-'}
                                    logging.debug(f"[Attempt {attempt+1}] Resuming {filename} from byte {start_byte}")
                                except OSError as e:
                                    logging.warning(f"[Attempt {attempt+1}] Could not get size of temp file {temp_filepath}: {e}. Starting fresh.")
                                    start_byte = 0
                                    resume_header = {}

                            logging.debug(f"[Attempt {attempt+1}] Acquiring semaphore for {filename}...")
                            async with self.semaphore:
                                logging.debug(f"[Attempt {attempt+1}] Semaphore acquired for {filename}. Making HTTP request...")
                                try:
                                    async with session.get(url, headers=resume_header, timeout=self.timeout) as response:
                                        logging.debug(f"[Attempt {attempt+1}] Received response headers for {filename} (Status: {response.status}). Starting to read body...")

                                        if response.status == 416:
                                            if os.path.exists(temp_filepath):
                                                logging.info(f"[Attempt {attempt+1}] HTTP 416 for {filename}, temp file exists, assuming complete.")
                                                os.rename(temp_filepath, filepath)
                                                async with self._lock:
                                                    self.downloaded_files.add(relative_path)
                                                    self.successfully_downloaded_count += 1
                                                success = True
                                                break # 成功处理，跳出重试循环
                                            else:
                                                raise aiohttp.ClientResponseError(
                                                    request_info=response.request_info,
                                                    history=response.history,
                                                    status=response.status,
                                                    message=f"HTTP 416 Range Not Satisfiable and no temp file for {filename}"
                                                )
                                        elif response.status not in (200, 206):
                                            raise aiohttp.ClientResponseError(
                                                request_info=response.request_info,
                                                history=response.history,
                                                status=response.status,
                                                message=f"HTTP error {response.status} for {filename}"
                                            )

                                        mode = 'ab' if start_byte > 0 else 'wb'
                                        logging.debug(f"[Attempt {attempt+1}] Opening file {temp_filepath} in mode '{mode}' for {filename}")

                                        with open(temp_filepath, mode) as f:
                                            async for chunk in response.content.iter_chunked(self.chunk_size):
                                                if chunk:
                                                    f.write(chunk)
                                                    f.flush()
                                                    os.fsync(f.fileno())
                                                    progress_bar.update(len(chunk))
                                            logging.debug(f"[Attempt {attempt+1}] Finished reading response body for {filename}.")

                                        final_temp_size = os.path.getsize(temp_filepath)
                                        total_size_str = response.headers.get('content-length', 'unknown')
                                        try:
                                            total_size = int(total_size_str) if total_size_str != 'unknown' else None
                                        except ValueError:
                                            total_size = None

                                        final_temp_size_kb = final_temp_size / 1024

                                        if total_size and final_temp_size >= total_size:
                                            os.rename(temp_filepath, filepath)
                                            async with self._lock:
                                                self.downloaded_files.add(relative_path)
                                                self.successfully_downloaded_count += 1
                                            success = True
                                            logging.info(f"[Attempt {attempt+1}] Successfully downloaded/resumed {filename} ({final_temp_size_kb:.2f} KB) to {relative_path}")
                                            break # 成功处理，跳出重试循环
                                        elif not total_size:
                                            os.rename(temp_filepath, filepath)
                                            async with self._lock:
                                                self.downloaded_files.add(relative_path)
                                                self.successfully_downloaded_count += 1
                                            success = True
                                            logging.info(f"[Attempt {attempt+1}] Downloaded {filename} (size unknown, assumed complete) to {relative_path}")
                                            break # 成功处理，跳出重试循环
                                        else:
                                            expected_kb = total_size / 1024 if total_size else 0
                                            got_kb = final_temp_size / 1024
                                            error_msg = f"Incomplete download for {filename}. Expected >= {expected_kb:.2f} KB, got {got_kb:.2f} KB"
                                            logging.warning(f"[Attempt {attempt+1}] {error_msg}")

                                except asyncio.TimeoutError:
                                    logging.warning(f"[Attempt {attempt+1}] TIMEOUT occurred for {filename} during HTTP request or response reading.")
                                except aiohttp.ClientError as e:
                                    logging.warning(f"[Attempt {attempt+1}] Client error for {filename}: {e}")
                                except Exception as e:
                                    logging.error(f"[Attempt {attempt+1}] Unexpected error for {filename}: {e}", exc_info=True)

                        # --- 单次尝试的 finally 块 ---
                        finally:
                            pass # 单次尝试的清理（如果需要）可以放在这里

                        # --- 重试逻辑 ---
                        if not success and attempt < self.retries - 1:
                            wait_time = 2 ** attempt
                            logging.debug(f"[Attempt {attempt+1}] Waiting {wait_time}s before retrying {filename}...")
                            await asyncio.sleep(wait_time)

                # --- 单个文件所有重试都结束后 ---
                if not success:
                    error_msg = f"Failed after {self.retries} attempts"
                    self.failed_files[relative_path] = error_msg
                    logging.error(f"Final failure for {filename} (intended path: {relative_path}): {error_msg}")
                    if os.path.exists(temp_filepath):
                        try:
                            os.remove(temp_filepath)
                            logging.debug(f"Cleaned up temporary file for failed download: {temp_filepath}")
                        except OSError as e:
                            logging.warning(f"Could not remove temporary file {temp_filepath}: {e}")
                # else: success 已在上面处理

            # --- 处理单个队列项目的 finally 块 ---
            # *** 关键：无论内部如何退出，都只在这里调用一次 task_done ***
            finally:
                # *** 确保 task_done 只为当前 get() 调用一次 ***
                try:
                    self.download_queue.task_done()
                    logging.debug(f"Task done for item related to {filename if 'filename' in locals() else 'unknown/shutdown'}.")
                except ValueError as e:
                    # 防御性：捕获重复调用的错误
                    logging.critical(f"CRITICAL: task_done() error for {filename if 'filename' in locals() else 'unknown/shutdown'}: {e}")
                except Exception as e:
                    # 捕获其他可能的异常
                    logging.error(f"Unexpected error in task_done() for {filename if 'filename' in locals() else 'unknown/shutdown'}: {e}")


    async def run(self):
        logging.info("Starting PDB Downloader...")
        logging.info(f"Output directory: {self.output_dir}")
        logging.info(f"Max files per directory: {self.max_files_per_dir}")
        start_time = time.time()

        self.download_queue = asyncio.Queue(maxsize=self.max_connections * 2)
        self._lock = asyncio.Lock()

        progress_bar = tqdm(
            total=0,
            unit='B',
            unit_scale=True,
            desc="Downloading PDB files"
        )

        async with aiohttp.ClientSession(timeout=self.timeout) as session:
            producer_task = asyncio.create_task(self.parse_html_producer())

            consumer_tasks = [
                asyncio.create_task(self.download_file_consumer(session, progress_bar))
                for _ in range(self.max_connections)
            ]

            await producer_task
            logging.info("Producer finished.")

            logging.info("Waiting for all download tasks in queue to complete...")
            await self.download_queue.join()
            logging.info("All download tasks completed (queue.join finished).")

            logging.info("Cancelling consumer tasks...")
            for task in consumer_tasks:
                if not task.done():
                    task.cancel()
            await asyncio.gather(*consumer_tasks, return_exceptions=True)
            logging.info("Consumer tasks cancelled and cleaned up.")

        progress_bar.close()

        elapsed_time = time.time() - start_time
        failed_count = len(self.failed_files)
        success_count_report = self.successfully_downloaded_count

        logging.info("="*50)
        logging.info("Download Session Summary:")
        logging.info(f"  - Files identified for download (not skipped): {self.total_files_to_download}")
        logging.info(f"  - Successfully downloaded this session: {success_count_report}")
        logging.info(f"  - Failed downloads: {failed_count}")
        logging.info(f"  - Total time taken: {elapsed_time:.2f} seconds")
        logging.info("="*50)

        if self.failed_files:
            logging.error("List of failed downloads (relative paths):")
            for relative_path, error in self.failed_files.items():
                logging.error(f"  - {relative_path}: {error}")
        else:
             logging.info("No files failed during this session.")

        return success_count_report, failed_count


if __name__ == "__main__":
    HTML_FILE_PATH = "/pdb_index.html"
    OUTPUT_DIR = "./pdb_files"
    MAX_CONNECTIONS = 50
    RETRIES = 3
    TIMEOUT = 300
    MAX_FILES_PER_DIR = 20000 # EXFAT 限制

    downloader = PDBDownloader(
        html_file_path=HTML_FILE_PATH,
        output_dir=OUTPUT_DIR,
        max_connections=MAX_CONNECTIONS,
        retries=RETRIES,
        timeout=TIMEOUT,
        max_files_per_dir=MAX_FILES_PER_DIR
    )

    try:
        success_count, failed_count = asyncio.run(downloader.run())
        print(f"\nDownload finished. Summary: {success_count} succeeded, {failed_count} failed.")
    except KeyboardInterrupt:
        logging.info("\nDownload process interrupted by user.")
    except Exception as e:
        logging.critical(f"An unexpected error occurred: {e}", exc_info=True)