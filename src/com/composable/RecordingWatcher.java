package com.composable;

import static java.nio.file.StandardWatchEventKinds.*;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;

public class RecordingWatcher {
	Path directory;
	WatchService watcher;
	public RecordingWatcher(Path directory) throws IOException {
		try {
		this.watcher = FileSystems.getDefault().newWatchService();
		this.directory = directory;
		directory.register(watcher, ENTRY_CREATE, ENTRY_MODIFY);
		} catch (IOException e) {
			System.err.println("Could not watch the recordings directory");
		}
	}
	
	public void watch(RecordingObserver observer) {
		for (;;) {
			WatchKey key = null;
			try {
				key = watcher.take(); 
			} catch (InterruptedException e) {
				System.err.println("Watching interrupted");
				return;
			}
			
			for (WatchEvent<?> event : key.pollEvents()) {
				WatchEvent.Kind<?> kind = event.kind();
				if (kind == OVERFLOW) {
					continue;
				}
				WatchEvent<Path> ev = (WatchEvent<Path>)event;
				Path filename = ev.context();
				
				if (kind == ENTRY_CREATE) {
					observer.create(filename);
				} else if (kind == ENTRY_MODIFY) {
					observer.modify(filename);
				}
				
				try {
					Path child = directory.resolve(filename);
					if (!Files.probeContentType(child).equals("video/x-matroska")) {
						System.out.println("Non-mkv file detected "+filename.toString());
						continue;
					}
				} catch (IOException e) {
					System.err.println(e);
					continue;
				}
				System.out.println("File event "+filename+" of type "+kind);
			}
			
			boolean valid = key.reset();
			if (!valid) {
				break;
			}
		}
	}
}
