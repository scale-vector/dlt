from typing import List, Sequence, Tuple, Type
from itertools import groupby
from pathlib import Path

from dlt.common.utils import chunks
from dlt.common.file_storage import FileStorage
from dlt.common.configuration import NormalizeVolumeConfiguration
from dlt.common.storages.versioned_storage import VersionedStorage


class NormalizeStorage(VersionedStorage):

    STORAGE_VERSION = "1.0.0"
    EXTRACTED_FOLDER: str = "extracted"  # folder within the volume where extracted files to be normalized are stored
    EXTRACTED_FILE_EXTENSION = ".extracted.json"
    EXTRACTED_FILE_EXTENSION_LEN = len(EXTRACTED_FILE_EXTENSION)

    def __init__(self, is_owner: bool, C: Type[NormalizeVolumeConfiguration]) -> None:
        super().__init__(NormalizeStorage.STORAGE_VERSION, is_owner, FileStorage(C.NORMALIZE_VOLUME_PATH, "t", makedirs=is_owner))

    def initialize_storage(self) -> None:
        self.storage.create_folder(NormalizeStorage.EXTRACTED_FOLDER, exists_ok=True)

    def list_files_to_normalize_sorted(self) -> Sequence[str]:
        return sorted(self.storage.list_folder_files(NormalizeStorage.EXTRACTED_FOLDER))

    def get_grouped_iterator(self, files: Sequence[str]) -> "groupby[str, str]":
        return groupby(files, lambda f: NormalizeStorage.get_schema_name(f))

    @staticmethod
    def chunk_by_events(files: Sequence[str], max_events: int, processing_cores: int) -> List[Sequence[str]]:
        # should distribute ~ N events evenly among m cores with fallback for small amounts of events

        def count_events(file_name : str) -> int:
            # return event count from file name
            return NormalizeStorage.get_events_count(file_name)

        counts = list(map(count_events, files))
        # make a list of files containing ~max_events
        events_count = 0
        m = 0
        while events_count < max_events and m < len(files):
            events_count += counts[m]
            m += 1
        processing_chunks = round(m / processing_cores)
        if processing_chunks == 0:
            # return one small chunk
            return [files]
        else:
            # should return ~ amount of chunks to fill all the cores
            return list(chunks(files[:m], processing_chunks))

    @staticmethod
    def get_events_count(file_name: str) -> int:
        return NormalizeStorage._parse_extracted_file_name(file_name)[0]

    @staticmethod
    def get_schema_name(file_name: str) -> str:
        return NormalizeStorage._parse_extracted_file_name(file_name)[2]

    @staticmethod
    def build_extracted_file_name(schema_name: str, stem: str, event_count: int, load_id: str) -> str:
        # builds file name with the extracted data to be passed to normalize
        return f"{schema_name}_{stem}_{load_id}_{event_count}{NormalizeStorage.EXTRACTED_FILE_EXTENSION}"

    @staticmethod
    def _parse_extracted_file_name(file_name: str) -> Tuple[int, str, str]:
        # parse extracted file name and returns (events found, load id, schema_name)
        if not file_name.endswith(NormalizeStorage.EXTRACTED_FILE_EXTENSION):
            raise ValueError(file_name)

        parts = Path(file_name[:-NormalizeStorage.EXTRACTED_FILE_EXTENSION_LEN]).stem.split("_")
        return (int(parts[-1]), parts[-2], parts[0])