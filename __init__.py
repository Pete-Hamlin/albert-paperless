import json
import os
from datetime import datetime, timedelta
from pathlib import Path
from socket import timeout
from threading import Event, Thread
from time import sleep
from urllib import parse

import requests
from albert import *

md_iid = "2.1"
md_version = "2.0"
md_name = "Paperless"
md_description = "Manage saved documents via a paperless instance"
md_license = "MIT"
md_url = "https://github.com/Pete-Hamlin/albert-python"
md_maintainers = ["@Pete-Hamlin"]
md_lib_dependencies = ["requests"]


class Plugin(PluginInstance, GlobalQueryHandler, TriggerQueryHandler):
    iconUrls = [f"file:{Path(__file__).parent}/paperless.png"]
    limit = 100
    headers = {"User-Agent": "org.albert.paperless"}

    def __init__(self):
        TriggerQueryHandler.__init__(
            self,
            id=md_id,
            name=md_name,
            description=md_description,
            synopsis="<document>",
            defaultTrigger="pl ",
        )
        GlobalQueryHandler.__init__(self, id=md_id, name=md_name, description=md_description, defaultTrigger="pl ")
        PluginInstance.__init__(self, extensions=[self])

        self._instance_url = self.readConfig("instance_url", str) or "http://localhost:8000"
        self._username = self.readConfig("username", str) or ""
        self._password = self.readConfig("password", str) or ""
        self._download_path = self.readConfig("download_path", str) or "~/Downloads"

        self._filter_by_tags = self.readConfig("filter_by_tags", bool) or True
        self._filter_by_type = self.readConfig("filter_by_type", bool) or True
        self._filter_by_correspondent = self.readConfig("filter_by_correspondent", bool) or True
        self._filter_by_body = self.readConfig("filter_by_body", bool) or False

        self._cache_results = self.readConfig("cache_results", bool) or True
        self._cache_length = self.readConfig("cache_length", int) or 60
        self._auto_cache = self.readConfig("auto_cache", bool) or False

        self.cache_timeout = datetime.now()
        self.cache_file = self.cacheLocation / "paperless.json"
        self.cache_thread = Thread(target=self.cache_routine, daemon=True)
        self.thread_stop = Event()

        if not self._auto_cache:
            self.thread_stop.set()

        self.tag_file = self.dataLocation / "paperless-tags.json"
        self.type_file = self.dataLocation / " paperless-types.json"
        self.correspondent_file = self.dataLocation / " paperless-correspondents.json"

        self.cache_thread.start()

    @property
    def instance_url(self):
        return self._instance_url

    @instance_url.setter
    def instance_url(self, value):
        self._instance_url = value
        self.writeConfig("instance_url", value)

    @property
    def username(self):
        return self._username

    @username.setter
    def username(self, value):
        self._username = value
        self.writeConfig("username", value)

    @property
    def password(self):
        return self._password

    @password.setter
    def password(self, value):
        self._password = value
        self.writeConfig("password", value)

    @property
    def download_path(self):
        return self._download_path

    @download_path.setter
    def download_path(self, value):
        self._download_path = value
        self.writeConfig("download_path", value)

    @property
    def cache_results(self):
        return self._cache_results

    @cache_results.setter
    def cache_results(self, value):
        self._cache_results = value
        if not self._cache_results:
            # Cleanup cache file
            self.cache_file.unlink(missing_ok=True)
        self.writeConfig("cache_results", value)

    @property
    def cache_length(self):
        return self._cache_length

    @cache_length.setter
    def cache_length(self, value):
        self._cache_length = value
        self.cache_timeout = datetime.now()
        self.writeConfig("cache_length", value)

    @property
    def auto_cache(self):
        return self._auto_cache

    @auto_cache.setter
    def auto_cache(self, value):
        self._auto_cache = value
        if self._auto_cache and self._cache_results:
            self.thread_stop.clear()
        else:
            self.thread_stop.set()
        self.writeConfig("auto_cache", value)

    @property
    def filter_by_tags(self):
        return self._filter_by_tags

    @filter_by_tags.setter
    def filter_by_tags(self, value):
        self._filter_by_tags = value
        if self._filter_by_tags:
            self.refresh_tags()
        else:
            self.tag_file.unlink(missing_ok=True)
        self.writeConfig("filter_by_tags", value)

    @property
    def filter_by_type(self):
        return self._filter_by_type

    @filter_by_type.setter
    def filter_by_type(self, value):
        self._filter_by_type = value
        if self._filter_by_type:
            self.refresh_types()
        else:
            self.type_file.unlink(missing_ok=True)
        self.writeConfig("filter_by_type", value)

    @property
    def filter_by_correspondent(self):
        return self._filter_by_correspondent

    @filter_by_correspondent.setter
    def filter_by_correspondent(self, value):
        self._filter_by_correspondent = value
        if self._filter_by_correspondent:
            self.refresh_correspondents()
        else:
            self.correspondent_file.unlink(missing_ok=True)
        self.writeConfig("filter_by_correspondent", value)

    @property
    def filter_by_body(self):
        return self._filter_by_body

    @filter_by_body.setter
    def filter_by_body(self, value):
        self._filter_by_body = value
        self.writeConfig("filter_by_body", value)

    def configWidget(self):
        return [
            {"type": "lineedit", "property": "instance_url", "label": "URL"},
            {"type": "lineedit", "property": "username", "label": "Username"},
            {
                "type": "lineedit",
                "property": "password",
                "label": "Password",
                "widget_properties": {"echoMode": "Password"},
            },
            {"type": "lineedit", "property": "download_path", "label": "Download Path"},
            {"type": "checkbox", "property": "filter_by_tags", "label": "Filter by document tags"},
            {"type": "checkbox", "property": "filter_by_type", "label": "Filter by document type"},
            {"type": "checkbox", "property": "filter_by_correspondent", "label": "Filter by document correspondent"},
            {"type": "checkbox", "property": "filter_by_body", "label": "Filter by document body"},
            {"type": "checkbox", "property": "cache_results", "label": "Cache results locally"},
            {"type": "spinbox", "property": "cache_length", "label": "Cache length (minutes)"},
            {"type": "checkbox", "property": "auto_cache", "label": "Periodically cache documents"},
        ]

    def handleTriggerQuery(self, query):
        stripped = query.string.strip()
        if stripped:
            # avoid spamming server
            for _ in range(50):
                sleep(0.01)
                if not query.isValid:
                    return

            data = self.get_results()
            documents = (item for item in data if stripped in self.create_filters(item))
            items = [item for item in self.gen_items(documents)]
            query.add(items)
        else:
            query.add(
                StandardItem(
                    id=md_id, text=md_name, subtext="Search for a document in Paperless", iconUrls=self.iconUrls
                )
            )
            if self._cache_results:
                query.add(
                    StandardItem(
                        id=md_id,
                        text="Refresh cache",
                        subtext="Refresh cached documents",
                        iconUrls=["xdg:view-refresh"],
                        actions=[Action("refresh", "Refresh document cache", lambda: self.refresh_cache())],
                    )
                )

    def handleGlobalQuery(self, query):
        stripped = query.string.strip()
        if stripped and self.cache_file.is_file():
            data = (item for item in self.read_file(self.cache_file))
            documents = (item for item in data if stripped in self.create_filters(item))
            items = [RankItem(item=item, score=0) for item in self.gen_items(documents)]
            return items

    def read_file(self, file: Path):
        if file.is_file():
            with file.open("r") as f:
                return json.load(f)

    def write_file(self, file: Path, data: list[dict]):
        with file.open("w") as f:
            f.write(json.dumps(data))
        return (item for item in data)

    def create_filters(self, item: dict):
        filters = item["title"]
        if self._filter_by_tags:
            filters += item.get("tags") or ""
        if self._filter_by_type:
            filters += item.get("document_type") or ""
        if self._filter_by_correspondent:
            filters += item.get("correspondent") or ""
        if self._filter_by_body:
            filters += item.get("body") or ""
        return filters

    def gen_items(self, documents: object):
        for document in documents:
            preview_url = "{}/api/documents/{}/preview/".format(self._instance_url, document["id"])
            download_url = "{}/api/documents/{}/download/".format(self._instance_url, document["id"])
            yield StandardItem(
                id=md_id,
                text=document["title"],
                subtext=" - ".join(
                    [
                        document.get("document_type") or "No type",
                        document.get("correspondent") or "No Correspondent",
                        document.get("tags") or "No tags",
                    ]
                ),
                iconUrls=self.iconUrls,
                actions=[
                    Action("download", "Download document", lambda u=download_url: self.download_document(u)),
                    Action("open", "Open document in browser", lambda u=preview_url: openUrl(u)),
                    Action("copy", "Copy preview URL to clipboard", lambda u=preview_url: setClipboardText(u)),
                    Action("copy-dl", "Copy preview URL to clipboard", lambda u=download_url: setClipboardText(u)),
                ],
            )

    def download_document(self, url: str):
        response = requests.get(url, timeout=5, auth=(self._username, self._password))
        if response.ok:
            header = (
                response.headers.get("Content-Disposition").split("'")[1].replace(" ", "_") or "albert_paperless_dl.pdf"
            )
            local_file = Path(self._download_path).expanduser() / header
            with local_file.open(mode="wb") as dl_file:
                for chunk in response.iter_content(chunk_size=8192):
                    dl_file.write(chunk)
            os.system(f"xdg-open '{local_file}'")

    def parse_tags(self, tags: list[int]):
        return ",".join(self.parse_tag(tag) for tag in tags)

    def parse_tag(self, tag: int):
        if tag:
            parsed_file = self.read_file(self.tag_file)
            if not parsed_file:
                # Refetch tags and try again
                parsed_file = self.refresh_tags()
            try:
                return next(parsed["name"] for parsed in parsed_file if parsed["id"] == tag)
            except StopIteration:
                warning(f"Error parsing tag {tag}")
                return f"<tag-{tag}>"


    def parse_type(self, doctype: int):
        if doctype:
            parsed_file = self.read_file(self.type_file)
            if not parsed_file:
                # Refetch types and try again
                parsed_file = self.refresh_types()
            return next(parsed["name"] for parsed in parsed_file if parsed["id"] == doctype)

    def parse_correspondent(self, correspondent: int):
        if correspondent:
            parsed_file = self.read_file(self.correspondent_file)
            if not parsed_file:
                # Refetch types and try again
                parsed_file = self.refresh_correspondents()
            return next(parsed["name"] for parsed in parsed_file if parsed["id"] == correspondent)

    def get_results(self):
        if self._cache_results:
            return self._get_cached_results()
        return self.fetch_documents()

    def _get_cached_results(self):
        if self.cache_file.is_file() and self.cache_timeout >= datetime.now():
            debug("Cache hit")
            results = self.read_file(self.cache_file)
            return (item for item in results)
        debug("Cache miss")
        return self.refresh_cache()

    def fetch_documents(self):
        params = {"limit": self.limit}
        url = f"{self._instance_url}/api/documents/?{parse.urlencode(params)}"
        documents = (document for document_list in self.fetch_request(url) for document in document_list)

        # This allows us to only run expensive parse functions once at the point of data ingress
        documents = self.field_map(documents, "document_type", self.parse_type)
        documents = self.field_map(documents, "correspondent", self.parse_correspondent)
        documents = self.field_map(documents, "tags", self.parse_tags)

        return documents

    def field_map(self, seq: object, field: str, func: object):
        for item in seq:
            if item.get(field):
                item[field] = func(item[field])
            yield item

    def fetch_tags(self):
        url = f"{self._instance_url}/api/tags/"
        return (tag for tag_list in self.fetch_request(url) for tag in tag_list)

    def fetch_types(self):
        url = f"{self._instance_url}/api/document_types/"
        return (doctype for type_list in self.fetch_request(url) for doctype in type_list)

    def fetch_correspondents(self):
        url = f"{self._instance_url}/api/correspondents/"
        return (correspondent for corr_list in self.fetch_request(url) for correspondent in corr_list)

    def refresh_cache(self):
        results = self.fetch_documents()
        self.cache_timeout = datetime.now() + timedelta(minutes=self._cache_length)
        return self.write_file(self.cache_file, [item for item in results])

    def cache_routine(self):
        while True:
            if not self.thread_stop.is_set():
                self.refresh_cache()
            sleep(3600)

    def refresh_tags(self):
        return self.write_file(self.tag_file, [tag for tag in self.fetch_tags()])

    def refresh_types(self):
        return self.write_file(self.type_file, [doc_type for doc_type in self.fetch_types()])

    def refresh_correspondents(self):
        return self.write_file(self.correspondent_file, [correspondent for correspondent in self.fetch_correspondents()])

    def fetch_request(self, url: str):
        while url:
            try:
                debug(f"GET request to {url}")
                response = requests.get(url, headers=self.headers, timeout=5, auth=(self._username, self._password))
                debug(f"Got response {response.status_code}")
                if response.ok:
                    result = response.json()
                    url = result["next"]
                    yield result["results"]
                else:
                    warning(f"Got response {response.status_code} querying {url}")
            except timeout:
                warning(f"Connection timed out for {url} - exiting")
                break
