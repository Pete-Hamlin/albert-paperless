import os
from datetime import datetime
from pathlib import Path
from threading import Event, Thread
from time import perf_counter_ns
from urllib import parse

import requests
from albert import *

md_iid = "2.3"
md_version = "3.3"
md_name = "Paperless"
md_description = "Manage saved documents via a paperless instance"
md_license = "MIT"
md_url = "https://github.com/Pete-Hamlin/albert-python"
md_authors = ["@Pete-Hamlin"]
md_lib_dependencies = ["requests"]


class DocumentFetcherThread(Thread):
    def __init__(self, callback, cache_length, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.__stop_event = Event()
        self.__callback = callback
        self.__cache_length = cache_length * 60

    def run(self):
        while True:
            self.__stop_event.wait(self.__cache_length)
            if self.__stop_event.is_set():
                return
            self.__callback()

    def stop(self):
        self.__stop_event.set()


class Plugin(PluginInstance, IndexQueryHandler):
    iconUrls = [f"file:{Path(__file__).parent}/paperless.png"]
    limit = 250
    user_agent = "org.albert.paperless"

    def __init__(self):
        PluginInstance.__init__(self)
        IndexQueryHandler.__init__(
            self, id=self.id, name=self.name, description=self.description, synopsis="<document>", defaultTrigger="pl "
        )

        self._instance_url = self.readConfig("instance_url", str) or "http://localhost:8000"
        self._api_key = self.readConfig("api_key", str) or ""
        self._download_path = self.readConfig("download_path", str) or "~/Downloads"

        self._filter_by_tags = self.readConfig("filter_by_tags", bool) or True
        self._filter_by_type = self.readConfig("filter_by_type", bool) or True
        self._filter_by_correspondent = self.readConfig("filter_by_correspondent", bool) or True
        self._filter_by_body = self.readConfig("filter_by_body", bool) or False

        self._cache_length = self.readConfig("cache_length", int) or 60

        self._tags = []
        self._types = []
        self._correspondents = []

        self.updateIndexItems()
        self._thread = DocumentFetcherThread(callback=self.updateIndexItems, cache_length=self._cache_length)
        self._thread.start()

    def __del__(self):
        self._thread.stop()
        self._thread.join()

    @property
    def instance_url(self):
        return self._instance_url

    @instance_url.setter
    def instance_url(self, value):
        self._instance_url = value
        self.writeConfig("instance_url", value)

    @property
    def api_key(self):
        return self._api_key

    @api_key.setter
    def api_key(self, value):
        self._api_key = value
        self.writeConfig("api_key", value)

    @property
    def download_path(self):
        return self._download_path

    @download_path.setter
    def download_path(self, value):
        self._download_path = value
        self.writeConfig("download_path", value)

    @property
    def cache_length(self):
        return self._cache_length

    @cache_length.setter
    def cache_length(self, value):
        self._cache_length = value
        self.cache_timeout = datetime.now()
        self.writeConfig("cache_length", value)

        if self._thread.is_alive():
            self._thread.stop()
            self._thread.join()
        self._thread = DocumentFetcherThread(callback=self.updateIndexItems, cache_length=self._cache_length)
        self._thread.start()

    @property
    def filter_by_tags(self):
        return self._filter_by_tags

    @filter_by_tags.setter
    def filter_by_tags(self, value):
        self._filter_by_tags = value
        self.writeConfig("filter_by_tags", value)

    @property
    def filter_by_type(self):
        return self._filter_by_type

    @filter_by_type.setter
    def filter_by_type(self, value):
        self._filter_by_type = value
        self.writeConfig("filter_by_type", value)

    @property
    def filter_by_correspondent(self):
        return self._filter_by_correspondent

    @filter_by_correspondent.setter
    def filter_by_correspondent(self, value):
        self._filter_by_correspondent = value
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
            {
                "type": "lineedit",
                "property": "api_key",
                "label": "API key",
                "widget_properties": {"echoMode": "Password"},
            },
            {"type": "lineedit", "property": "download_path", "label": "Download Path"},
            {"type": "checkbox", "property": "filter_by_tags", "label": "Filter by document tags"},
            {"type": "checkbox", "property": "filter_by_type", "label": "Filter by document type"},
            {"type": "checkbox", "property": "filter_by_correspondent", "label": "Filter by document correspondent"},
            {"type": "checkbox", "property": "filter_by_body", "label": "Filter by document body"},
            {"type": "spinbox", "property": "cache_length", "label": "Cache length (minutes)"},
        ]


    def updateIndexItems(self):
        start = perf_counter_ns()
        data = self._fetch_documents()
        index_items = []
        for document in data:
            filter = self._create_filters(document)
            item = self._gen_item(document)
            index_items.append(IndexItem(item=item, string=filter))
        self.setIndexItems(index_items)
        info("Indexed {} documents [{:d} ms]".format(len(index_items), (int(perf_counter_ns() - start) // 1000000)))

    def handleTriggerQuery(self, query):
        stripped = query.string.strip()
        if stripped:
            TriggerQueryHandler.handleTriggerQuery(self, query)
            query.add(
                StandardItem(
                    text="Refresh cache index",
                    subtext="Refresh indexed links",
                    iconUrls=["xdg:view-refresh"],
                    actions=[Action("refresh", "Refresh paperless index", lambda: self.updateIndexItems())],
                )
            )
        else:
            query.add(
                StandardItem(
                    text=self.name, subtext="Search for a document in Paperless", iconUrls=self.iconUrls
                )
            )

    def _create_filters(self, item: dict):
        filters = item["title"]
        if self._filter_by_tags:
            filters += "," + str(item.get("tags"))
        if self._filter_by_type:
            filters += "," + str(item.get("document_type"))
        if self._filter_by_correspondent:
            filters += "," + str(item.get("correspondent"))
        if self._filter_by_body:
            filters += "," + str(item.get("body"))
        return filters.lower()

    def _gen_item(self, document: object):
        preview_url = "{}/api/documents/{}/preview/".format(self._instance_url, document["id"])
        download_url = "{}/api/documents/{}/download/".format(self._instance_url, document["id"])
        return StandardItem(
            id=self.id,
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
                Action("download", "Download document", lambda u=download_url: self._download_document(u)),
                Action("open", "Open document in browser", lambda u=preview_url: openUrl(u)),
                Action("copy", "Copy preview URL to clipboard", lambda u=preview_url: setClipboardText(u)),
                Action("copy-dl", "Copy preview URL to clipboard", lambda u=download_url: setClipboardText(u)),
            ],
        )

    def _download_document(self, url: str):
        headers = {"User-Agent": self.user_agent, "Authorization": f"Token {self._api_key}"}
        response = requests.get(url, timeout=5, headers=headers)
        if response.ok:
            header = (
                response.headers.get("Content-Disposition").split("'")[1].replace(" ", "_") or "albert_paperless_dl.pdf"
            )
            local_file = Path(self._download_path).expanduser() / header
            with local_file.open(mode="wb") as dl_file:
                for chunk in response.iter_content(chunk_size=8192):
                    dl_file.write(chunk)
            os.system(f"xdg-open '{local_file}'")

    def _parse_tags(self, tags: list[int]):
        return ",".join(self._parse_tag(tag) for tag in tags)

    def _parse_tag(self, tag: int):
        if tag:
            try:
                return next(parsed["name"] for parsed in self._tags if parsed["id"] == tag)
            except StopIteration:
                warning(f"Error parsing tag {tag}")
                return f"<tag-{tag}>"

    def _parse_type(self, doctype: int):
        if doctype:
            return next(parsed["name"] for parsed in self._types if parsed["id"] == doctype)

    def _parse_correspondent(self, correspondent: int):
        if correspondent:
            return next(parsed["name"] for parsed in self._correspondents if parsed["id"] == correspondent)

    def _fetch_documents(self):
        params = {"page_size": self.limit}
        url = f"{self._instance_url}/api/documents/?{parse.urlencode(params)}"
        documents = (document for document_list in self._fetch_request(url) for document in document_list)

        # This allows us to only run expensive parse functions once at the point of data ingress
        if self._filter_by_tags:
            self._tags = self._fetch_tags(params)
            documents = self._field_map(documents, "tags", self._parse_tags)
        if self._filter_by_type:
            self._types = self._fetch_types(params)
            documents = self._field_map(documents, "document_type", self._parse_type)
        if self._filter_by_correspondent:
            self._correspondents = self._fetch_correspondents(params)
            documents = self._field_map(documents, "correspondent", self._parse_correspondent)

        return documents

    def _field_map(self, seq: object, field: str, callback: object):
        for item in seq:
            if item.get(field):
                item[field] = callback(item[field])
            yield item

    def _fetch_tags(self, params):
        url = f"{self._instance_url}/api/tags/?{parse.urlencode(params)}"
        return [tag for tag_list in self._fetch_request(url) for tag in tag_list]

    def _fetch_types(self, params):
        url = f"{self._instance_url}/api/document_types/?{parse.urlencode(params)}"
        return [doctype for type_list in self._fetch_request(url) for doctype in type_list]

    def _fetch_correspondents(self, params):
        url = f"{self._instance_url}/api/correspondents/?{parse.urlencode(params)}"
        return [correspondent for corr_list in self._fetch_request(url) for correspondent in corr_list]

    def _fetch_request(self, url: str):
        while url:
            debug(f"GET request to {url}")
            headers = {"User-Agent": self.user_agent, "Authorization": f"Token {self._api_key}"}
            response = requests.get(url, headers=headers, timeout=5)
            debug(f"Got response {response.status_code}")
            if response.ok:
                result = response.json()
                url = result["next"]
                yield result["results"]
            else:
                warning(f"Got response {response.status_code} querying {url}")
                url = None
