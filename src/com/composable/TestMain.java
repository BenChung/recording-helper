package com.composable;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import net.samuelcampos.usbdrivedetector.USBDeviceDetectorManager;
import net.samuelcampos.usbdrivedetector.events.DeviceEventType;
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
		System.err.println("version 0.1.4 10/22/19");
		
		RecordingManager toTest = new RecordingManager(Paths.get(db), machineName);
		detectDrives(machineName, Paths.get(videosPath), toTest);
		watchVideos(videosPath, toTest);
	}

	private static void detectDrives(String machineName, Path videosPath, RecordingManager toTest) {
		USBDeviceDetectorManager driveDetector = new USBDeviceDetectorManager();
		driveDetector.addDriveListener((USBStorageEvent evt) -> {
			if (evt.getEventType() != DeviceEventType.CONNECTED) return;
			synchronized (toTest) { 
				TestMain.CopyExports(machineName, videosPath, evt.getStorageDevice().getRootDirectory().toPath(), toTest);
			}
		});
	}

	private static void watchVideos(String videosPath, RecordingManager toTest) {
		RecordingWatcher rw;
		Path vp = Paths.get(videosPath);
		try {
			rw = new RecordingWatcher(vp);
		} catch (IOException e) {
			System.err.println("Watcher IO xception");
			e.printStackTrace();
			return;
		}
		rw.watch(new TestMain.VideoObserver(toTest, vp));
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
		private Path videoPath;

		VideoObserver(RecordingManager toTest, Path videoPath) {
			this.toTest = toTest; 
			this.videoPath = videoPath;
		}

		@Override
		public void create(Path filename) {
			synchronized(toTest){
				toTest.NewVideo(videoPath.resolve(filename));
			}
		}

		@Override
		public void modify(Path filename) {
			synchronized(toTest) {
				toTest.EndVideo(videoPath.resolve(filename));
			}
		}
	}

	public static void CopyExports(String machinename, Path videosPath, Path extdd, RecordingManager recordings) {
		ArrayList<Path> exports = recordings.ToExport();
		if (exports.size() == 0) {
			System.err.println("no new recordings");
			return; // no new recordings to export
		}
		String dirname = machinename + "-" + System.currentTimeMillis();
		Path tgtdir;
		try {
			tgtdir = Files.createDirectory(extdd.resolve(Paths.get(dirname)));
		} catch (IOException e1) {
			System.err.println("Could not create target directory "+dirname);
			e1.printStackTrace();
			return;
		}
		System.err.println("writing export manifest");
		recordings.WriteExportManifest(tgtdir.resolve(Paths.get("manifest.csv")));
		System.err.println("exporting");
		for (Path export : exports) {
			try {
				Path iexport = videosPath.resolve(export);
				Path outp_path = tgtdir.resolve(Path.of(export.getFileName().toString()));

				String inp_md5;
				try (InputStream is = Files.newInputStream(iexport)) {
				    inp_md5 = org.apache.commons.codec.digest.DigestUtils.md5Hex(is);
				}
				
				int tries = 0;
				boolean successful = false;
				while (tries < 5) {
					tries++;
					System.err.println("writing "+iexport+" to "+outp_path);
					Files.copy(iexport, outp_path, StandardCopyOption.REPLACE_EXISTING);
					if (Files.size(iexport) != Files.size(outp_path)) {
						continue;
					}
					String outp_md5;
					try (InputStream is = Files.newInputStream(outp_path)) {
					    outp_md5 = org.apache.commons.codec.digest.DigestUtils.md5Hex(is);
					}
					if (!inp_md5.equals(outp_md5)) {
						continue;
					}
					successful = true;
					break;
				}
				if (successful) recordings.SetExported(export);
			} catch (IOException e) {
				System.err.println("Failed to export "+export);
				e.printStackTrace();
			}
		}
		System.err.println("done exporting");
	}
}
