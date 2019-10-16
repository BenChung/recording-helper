package com.composable;

import java.nio.file.Path;

public interface RecordingObserver {
	void create(Path filename);
	void modify(Path filename);
}
