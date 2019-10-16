package com.composable;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import net.samuelcampos.usbdrivedetector.USBDeviceDetectorManager;
import net.samuelcampos.usbdrivedetector.events.USBStorageEvent;

public class TestMain {

	public static void main(String[] args) {
		// drive detector will run in its own thread and will write from its thread if it can
		// we'll protect toTest with locks
		CommandLine cmd = parseArgs(args);
		
		String machineName = cmd.getOptionValue("nm");
		String db = cmd.getOptionValue("db");
		String videosPath = cmd.getOptionValue("obs");
		if (machineName==null || db==null || videosPath==null) {
			return;
		}
		
		RecordingManager toTest = new RecordingManager(Paths.get(db), machineName);
		detectDrives(machineName, toTest);
		watchVideos(videosPath, toTest);
	}

	private static void detectDrives(String machineName, RecordingManager toTest) {
		USBDeviceDetectorManager driveDetector = new USBDeviceDetectorManager();
		driveDetector.addDriveListener((USBStorageEvent evt) -> {
			synchronized (toTest) {
				TestMain.CopyExports(machineName, evt.getStorageDevice().getRootDirectory().toPath(), toTest);
			}
		});
	}

	private static void watchVideos(String videosPath, RecordingManager toTest) {
		RecordingWatcher rw;
		try {
			rw = new RecordingWatcher(Paths.get(videosPath));
		} catch (IOException e) {
			System.err.println("Watcher IO xception");
			e.printStackTrace();
			return;
		}
		rw.watch(new TestMain.VideoObserver(toTest));
	}
	
	private static CommandLine parseArgs(String[] args) {

		Options options = new Options();
		options.addOption("obs", true, "OBS target directory");
		options.addOption("db", "database", true, "SQLite database file (auto-created)");
		options.addOption("nm", "name", true, "Machine name");
		CommandLine cmd;
		try {
			cmd = new DefaultParser().parse(options, args);
		} catch (ParseException e1) {
			throw new RuntimeException(e1);
		}

		String machineName = cmd.getOptionValue("nm");
		String db = cmd.getOptionValue("db");
		String videosPath = cmd.getOptionValue("obs");
		
		if (machineName==null || db==null || videosPath==null) {
			new HelpFormatter().printHelp("watcher", options);
		}
		return cmd;
	}

	public static final class VideoObserver implements RecordingObserver {
		private final RecordingManager toTest;

		VideoObserver(RecordingManager toTest) {
			this.toTest = toTest;
		}

		@Override
		public void create(Path filename) {
			synchronized(toTest){
				toTest.NewVideo(filename);
			}
		}

		@Override
		public void modify(Path filename) {
			synchronized(toTest) {
				toTest.EndVideo(filename);
			}
		}
	}

	public static void CopyExports(String machinename, Path extdd, RecordingManager recordings) {
		String dirname = machinename + "-" + System.currentTimeMillis();
		Path tgtdir;
		try {
			tgtdir = Files.createDirectory(extdd.resolve(Paths.get(dirname)));
		} catch (IOException e1) {
			System.err.println("Could not create target directory "+dirname);
			e1.printStackTrace();
			return;
		}
		ArrayList<Path> exports = recordings.ToExport();
		System.err.println("writing export manifest");
		recordings.WriteExportManifest(tgtdir.resolve(Paths.get("manifest.csv")));
		System.err.println("exporting");
		for (Path export : exports) {
			try {
				Files.copy(export, tgtdir.resolve(Path.of(export.getFileName().toString())));
				recordings.SetExported(export);
			} catch (IOException e) {
				System.err.println("Failed to export "+export);
				e.printStackTrace();
			}
		}
		System.err.println("done exporting");
	}
}
