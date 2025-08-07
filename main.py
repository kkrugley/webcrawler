import asyncio
import os
import re
from urllib.parse import urlparse, urljoin
from pathlib import Path

# --- Визуализация и интерактивность ---
from rich.console import Console
from rich.prompt import Prompt, IntPrompt, Confirm
from rich.panel import Panel
from rich.progress import Progress, SpinnerColumn, BarColumn, TextColumn, TimeRemainingColumn
# ------------------------------------

from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError
from pypdf import PdfWriter

# --- НАСТРОЙКИ МАСКИРОВКИ ---
FAKE_USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36"
EXTRA_HEADERS = {
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
    'Accept-Language': 'en-US,en;q=0.9,ru;q=0.8',
    'Accept-Encoding': 'gzip, deflate, br',
    'Sec-Ch-Ua': '"Chromium";v="118", "Google Chrome";v="118", "Not=A?Brand";v="99"',
    'Sec-Ch-Ua-Mobile': '?0',
    'Sec-Ch-Ua-Platform': '"Windows"',
    'Sec-Fetch-Dest': 'document',
    'Sec-Fetch-Mode': 'navigate',
    'Sec-Fetch-Site': 'none',
    'Sec-Fetch-User': '?1',
    'Upgrade-Insecure-Requests': '1',
}
# -----------------------------

# Инициализируем консоль Rich для красивого вывода
console = Console()

def get_user_config() -> dict:
    """Запрашивает конфигурацию у пользователя в интерактивном режиме."""
    console.print(Panel.fit("[bold cyan]Добро пожаловать в Web-To-PDF Crawler![/bold cyan]\nДавайте настроим параметры для обхода сайта.", title="Настройка"))

    start_url = Prompt.ask("[yellow]Введите стартовый URL[/yellow]", default="https://kkrugley.github.io/")

    # Удобный выбор режима скраппинга по номеру
    crawl_mode_map = {"1": "Стандартный", "2": "Sitemap.xml"}
    crawl_mode_choice = Prompt.ask(
        "[yellow]Выберите режим скраппинга[/yellow] ([bold]1[/bold]=Стандартный, [bold]2[/bold]=Sitemap.xml)",
        choices=["1", "2"],
        default="1"
    )
    crawl_mode = crawl_mode_map[crawl_mode_choice]

    max_depth = IntPrompt.ask("[yellow]Введите максимальную глубину обхода[/yellow]", default=1)
    request_delay = IntPrompt.ask("[yellow]Введите задержку между запросами (сек)[/yellow]", default=2)

    export_format = Prompt.ask(
        "[yellow]Экспортировать в формате PDF или экспортировать только текст?[/yellow]",
        choices=["PDF", "Text"],
        default="Text"
    )

    merge_files = Confirm.ask("[yellow]Объединять загруженные файлы?[/yellow]", default=True)

    output_dir = "output_pdfs" if export_format == "PDF" else "output_texts"
    merged_filename = "merged_output.pdf" if export_format == "PDF" else "merged_output.md"

    return {
        "START_URL": start_url,
        "CRAWL_MODE": crawl_mode,
        "MAX_DEPTH": max_depth,
        "REQUEST_DELAY": request_delay,
        "EXPORT_FORMAT": export_format,
        "OUTPUT_DIR": output_dir,
        "MERGED_FILENAME": merged_filename,
        "DELETE_INDIVIDUAL_FILES": True,
        "MERGE_FILES": merge_files,
    }

def sanitize_filename(title: str) -> str:
    """Очищает строку, чтобы она была валидным именем файла."""
    if not title:
        title = "no_title"
    # Удаляем недопустимые символы для Windows/Linux/macOS
    sanitized = re.sub(r'[\\/*?:"<>|]', "", title)
    # Заменяем пробелы и длинные последовательности дефисов
    sanitized = re.sub(r'\s+', '_', sanitized)
    sanitized = re.sub(r'__+', '_', sanitized)
    return sanitized[:100] # Ограничиваем длину имени файла

def is_valid_url(url: str, start_hostname: str, start_path: str) -> bool:
    """
    Проверяет, соответствует ли URL строгим критериям:
    1. Тот же хост (без поддоменов).
    2. Путь начинается с того же начального пути.
    3. Не является ссылкой на файл.
    """
    try:
        parsed_url = urlparse(url)
        if parsed_url.scheme not in ['http', 'https']: return False
        if parsed_url.netloc != start_hostname: return False
        if not parsed_url.path.startswith(start_path): return False
        if any(parsed_url.path.lower().endswith(ext) for ext in ['.pdf', '.zip', '.jpg', '.png', '.gif', '.xml', '.rss', '.gz']): return False
        return True
    except ValueError:
        return False


import aiohttp
import xml.etree.ElementTree as ET

async def fetch_sitemap_links(sitemap_url):
    """Загружает и парсит sitemap.xml, возвращает список ссылок."""
    links = []
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(sitemap_url, headers=EXTRA_HEADERS) as resp:
                if resp.status == 200:
                    text = await resp.text()
                    try:
                        root = ET.fromstring(text)
                        for url in root.findall('.//{*}loc'):
                            if url.text:
                                loc = url.text.strip()
                                if loc:
                                    links.append(loc)
                    except Exception:
                        pass
    except Exception:
        pass
    return links

async def main(CONFIG: dict):
    """Главная асинхронная функция с визуализацией."""
    output_path = Path(CONFIG["OUTPUT_DIR"])
    output_path.mkdir(exist_ok=True)

    start_url = CONFIG["START_URL"]
    parsed_start_url = urlparse(start_url)
    start_hostname = parsed_start_url.netloc

    start_path_obj = Path(parsed_start_url.path)
    start_path = str(start_path_obj.parent.as_posix()) if start_path_obj.suffix else str(start_path_obj.as_posix())
    if not start_path.endswith('/'):
        start_path += '/'

    queue = asyncio.Queue()
    visited = set()
    saved_files = []

    # --- Sitemap режим ---
    if CONFIG.get("CRAWL_MODE") == "Sitemap.xml":
        # Определяем URL sitemap.xml
        sitemap_url = start_url.rstrip('/') + "/sitemap.xml" if not start_url.endswith(".xml") else start_url
        console.print(f"[yellow]Загружаем sitemap: {sitemap_url}[/yellow]")
        links = await fetch_sitemap_links(sitemap_url)
        if not links:
            console.print("[red]Не удалось получить ссылки из sitemap.xml. Переход к стандартному режиму.[/red]")
            await queue.put((start_url, 0))
        else:
            for link in links:
                await queue.put((link, 0))
    else:
        await queue.put((start_url, 0))

    async with async_playwright() as p:
        console.print("[green]Запускаем браузер в фоновом режиме...[/green]")
        browser = await p.chromium.launch(headless=True, channel="chrome")
        context = await browser.new_context(
            user_agent=FAKE_USER_AGENT,
            extra_http_headers=EXTRA_HEADERS,
        )
        await context.add_init_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
        page = await context.new_page()

        console.print(f"""
[bold]Начинаем обход сайта со следующими параметрами:[/bold]
- [cyan]URL:[/cyan] {start_url}
- [cyan]Хост:[/cyan] {start_hostname}
- [cyan]Путь:[/cyan] {start_path}*
- [cyan]Глубина:[/cyan] {CONFIG['MAX_DEPTH']}
- [cyan]Формат:[/cyan] {CONFIG['EXPORT_FORMAT']}
        """)

        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
            TimeRemainingColumn(),
            console=console
        ) as progress:
            
            crawl_task = progress.add_task("[cyan]Обход страниц...", total=1)

            while not queue.empty():
                progress.update(crawl_task, total=len(visited) + queue.qsize())

                current_url, current_depth = await queue.get()
                current_url = urljoin(current_url, urlparse(current_url).path)

                if current_url in visited or current_depth > CONFIG["MAX_DEPTH"]:
                    continue

                visited.add(current_url)
                progress.update(crawl_task, description=f"[cyan]Обработка ({len(visited)}/{len(visited) + queue.qsize()})[/cyan] [white]{current_url[:70]}...[/white]")

                try:
                    await page.goto(current_url, wait_until="networkidle", timeout=60000)

                    # --- ОБРАБОТКА COOKIE БАННЕРА ---
                    try:
                        accept_button = page.get_by_role(
                            "button",
                            name=re.compile("Accept All Cookies|Accept", re.IGNORECASE)
                        )
                        await accept_button.click(timeout=3000)
                        progress.console.print("[bold green]  ✓ Приняли cookie-соглашение.[/bold green]")
                        await page.wait_for_timeout(1000)
                    except PlaywrightTimeoutError:
                        pass # Баннера нет, это нормально
                    # --------------------------------

                    page_title = await page.title()

                    # В стандартном режиме ищем новые ссылки
                    if CONFIG.get("CRAWL_MODE") != "Sitemap.xml" and current_depth < CONFIG["MAX_DEPTH"]:
                        links = await page.eval_on_selector_all("a", "elements => elements.map(el => el.href)")
                        for link in links:
                            absolute_url = urljoin(current_url, link)
                            if is_valid_url(absolute_url, start_hostname, start_path) and absolute_url not in visited and absolute_url not in [item[0] for item in queue._queue]:
                                await queue.put((absolute_url, current_depth + 1))

                    if CONFIG["EXPORT_FORMAT"] == "PDF":
                        pdf_filename = sanitize_filename(page_title) + ".pdf"
                        pdf_filepath = output_path / pdf_filename
                        await page.pdf(path=pdf_filepath, format="A4", print_background=True)
                        saved_files.append(str(pdf_filepath))
                    else: # Text
                        text_content = await page.evaluate("document.body.innerText")
                        md_filename = sanitize_filename(page_title) + ".md"
                        md_filepath = output_path / md_filename
                        with open(md_filepath, "w", encoding="utf-8") as f:
                            f.write(f"# {page_title}\n\n{text_content}")
                        saved_files.append(str(md_filepath))

                except Exception as e:
                    progress.console.print(f"[red]  [!] Ошибка на {current_url}: {e}[/red]")

                progress.update(crawl_task, advance=1)
                await asyncio.sleep(CONFIG["REQUEST_DELAY"])
        
        await context.close()
        await browser.close()

    if saved_files:
        if CONFIG["MERGE_FILES"]:
            console.print(f"\n[bold green]Обход завершен. Найдено {len(saved_files)} страниц. Начинаем слияние...[/bold green]")
            
            if CONFIG["EXPORT_FORMAT"] == "PDF":
                merger = PdfWriter()
                for pdf_path in sorted(saved_files):
                    try:
                        merger.append(pdf_path)
                    except Exception:
                        console.print(f"[yellow]  [!] Не удалось добавить файл {pdf_path}, пропускаем.[/yellow]")

                merged_filepath = CONFIG["MERGED_FILENAME"]
                merger.write(merged_filepath)
                merger.close()
                console.print(f"[bold magenta]🎉 Все страницы успешно объединены в один файл: {merged_filepath}[/bold magenta]")
            else: # Text merge
                merged_filepath = CONFIG["MERGED_FILENAME"]
                with open(merged_filepath, "w", encoding="utf-8") as merged_file:
                    for file_path in sorted(saved_files):
                        try:
                            with open(file_path, "r", encoding="utf-8") as f:
                                merged_file.write(f.read())
                                merged_file.write("\n\n---\n\n")
                        except Exception as e:
                            console.print(f"[yellow]  [!] Не удалось прочитать файл {file_path}: {e}[/yellow]")
                console.print(f"[bold magenta]🎉 Все страницы успешно объединены в один файл: {merged_filepath}[/bold magenta]")

            if CONFIG["DELETE_INDIVIDUAL_FILES"]:
                console.print("[dim]Удаляем временные файлы...[/dim]")
                # Используем set для удаления только уникальных файлов, избегая ошибок,
                # если несколько страниц имели одинаковый заголовок и перезаписали один и тот же файл.
                for file_path in set(saved_files):
                    try:
                        os.remove(file_path)
                    except OSError as e:
                        console.print(f"[red]  [!] Ошибка при удалении файла {file_path}: {e}[/red]")
        else:
            file_type = "PDF-файлов" if CONFIG["EXPORT_FORMAT"] == "PDF" else "MD-файлов"
            console.print(f"\n[bold green]Обход завершен. Сохранено {len(saved_files)} {file_type} в папке '{CONFIG['OUTPUT_DIR']}'.[/bold green]")
    else:
        file_type = "PDF" if CONFIG["EXPORT_FORMAT"] == "PDF" else "текстовых"
        console.print(f"\n[bold yellow]Не было создано ни одного {file_type} файла.[/bold yellow]")

    console.print("\n[bold]Работа скрипта завершена.[/bold]")

if __name__ == "__main__":
    try:
        config = get_user_config()
        asyncio.run(main(config))
    except KeyboardInterrupt:
        console.print("\n\n[bold red]Выполнение прервано пользователем.[/bold red]")