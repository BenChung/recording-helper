package com.composable;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.UUID;


public class RecordingManager {
	private Connection conn;
	private String machine_name;
	public RecordingManager(Path sqlite_db, String machine_name) {
		try {
			this.conn = DriverManager.getConnection("jdbc:sqlite:"+sqlite_db.toAbsolutePath());
		} catch (SQLException e) {
			System.err.println("SQL exception on DB creation");
			System.err.println(e);
		}
		this.machine_name = machine_name;
		UpdateSchema();
	}
	
	// schema
	// |       Recordings      |
	// | id : int primary key  |
	// | guid : string         | 
	// | filename : string     |
	// | start : datetime      |
	// | end : datetime        |
	// | machine : string      |
	
	// |       Saved           |
	// | id : int primary key  |
	// | rid : int foreign key |
	// | exported : bool       | 
	private void UpdateSchema() {
		String recordings_sql = 
				"CREATE TABLE IF NOT EXISTS Recordings ("
				+ "     id INTEGER PRIMARY KEY,"
				+ "     guid TEXT NOT NULL,"
				+ "     filename TEXT NOT NULL,"
				+ "     start DATETIME,"
				+ "     end DATETIME,"
				+ "     machine TEXT NOT NULL"
				+ ")";
		
		String saved_sql = 
				"CREATE TABLE IF NOT EXISTS Saved ("
				+ "     id INTEGER PRIMARY KEY,"
				+ "     rid INTEGER,"
				+ "     exported INTEGER,"
				+ "     FOREIGN KEY(rid) REFERENCES Recordings(id)"
				+ ")";
		try {
		Statement stmt = conn.createStatement();
		stmt.execute(recordings_sql);
		stmt.execute(saved_sql);
		} catch (SQLException e) {
			System.err.println("SQL exception on schema update/creation");
			throw new RuntimeException(e);
			
		}
	}

	private int CreateVideoEntry(Path video, Timestamp start) throws SQLException {
		String sql = "INSERT INTO Recordings (guid, filename, start, machine) VALUES (?, ?, ?, ?)";
		String guid = UUID.randomUUID().toString();
		PreparedStatement stmt = conn.prepareStatement(sql, PreparedStatement.RETURN_GENERATED_KEYS);
		stmt.setString(1, guid); // guid
		stmt.setString(2, video.toAbsolutePath().toString()); // filename
		stmt.setTimestamp(3, start); // start
		stmt.setString(4, machine_name); // machine
		stmt.executeUpdate();
		ResultSet rs = stmt.getGeneratedKeys();
		return rs.next() ? rs.getInt(1):0;
	}
	
	private int CreateVideoEntry(Path video) throws SQLException {
		String sql = "INSERT INTO Recordings (guid, filename, machine) VALUES (?, ?, ?)";
		String guid = UUID.randomUUID().toString();
		PreparedStatement stmt = conn.prepareStatement(sql, PreparedStatement.RETURN_GENERATED_KEYS);
		stmt.setString(1, guid); // guid
		stmt.setString(2, video.toAbsolutePath().toString()); // filename
		stmt.setString(3, machine_name); // machine
		stmt.executeUpdate();
		ResultSet rs = stmt.getGeneratedKeys();
		return rs.next() ? rs.getInt(1):0;
	}
	
	private void CreateSavedEntry(int colid) throws SQLException {
		String sql = "INSERT INTO Saved (rid, exported) VALUES (?,?)";
		PreparedStatement stmt = conn.prepareStatement(sql);
		stmt.setInt(1, colid);
		stmt.setInt(2, 0);
		stmt.execute();
	}
 	
	public void NewVideo(Path video) {
		// flow: 
		// check if video with same path exists -> if so, do nothing
		// create new video
		// create new saved entry (not saved)

		System.out.println("Initalizing "+video.toAbsolutePath()+" to "+ System.currentTimeMillis()); 
		try {
			String sql = "SELECT count(*) FROM Recordings WHERE filename=?";
			PreparedStatement stmt = conn.prepareStatement(sql);
			stmt.setString(1, video.toAbsolutePath().toString());
			ResultSet exSet = stmt.executeQuery();
			if (exSet.getInt(1) != 0) {
				System.out.println("update "+video);
				return;
			}
			// no existing video
			int colid = CreateVideoEntry(video, new Timestamp(System.currentTimeMillis()));
			// insert new update entry
			CreateSavedEntry(colid);
		} catch (SQLException e) {
			System.err.println("Error creating metadata for a video");
			e.printStackTrace();
		}
	}
	
	public void EndVideo(Path video) {
		// flow:
		// check if video exists -> if not, create video with end only
		// update video end to current time
		try {
		String sql = "SELECT id FROM Recordings WHERE filename=?";
		PreparedStatement stmt = conn.prepareStatement(sql);
		stmt.setString(1, video.toAbsolutePath().toString());
		ResultSet exSet = stmt.executeQuery();
		int id = 0;
		if (!exSet.next()) {
			// video DOES NOT exist; there was a failure! create new recording
			id = CreateVideoEntry(video);
			CreateSavedEntry(id);
		} else {
			id = exSet.getInt(1);
		}
		// set the end time
		sql = "UPDATE Recordings SET end=? WHERE id=?";
		stmt = conn.prepareStatement(sql);
		stmt.setTimestamp(1, new Timestamp(System.currentTimeMillis()));
		stmt.setInt(2, id);
		stmt.execute();

		System.out.println("Setting end time on video "+video.toAbsolutePath()+" to "+ System.currentTimeMillis()); 
		} catch (SQLException e) {
			System.err.println("Error editing metadata for a video");
			e.printStackTrace();
		}
	}
	
	public ArrayList<Path> ToExport() {
		// flow:
		// get all videos to export
		ArrayList<Path> outp = new ArrayList<Path>();
		try {
			String sql = "SELECT filename FROM Recordings INNER JOIN Saved ON Recordings.id = Saved.rid WHERE Saved.exported=0";
			PreparedStatement stmt = conn.prepareStatement(sql);
			ResultSet res = stmt.executeQuery();
			while (res.next()) {
				outp.add(Paths.get(res.getString(1)));
			}
		} catch (SQLException e) {
			System.err.println("Error reading videos for export");
			e.printStackTrace();
		}
		return outp;
	}
	
	public void SetExported(Path video) {
		// flow:
		// update saved to exported
		
		// get the id
		try {
		String sql = "SELECT id FROM Recordings WHERE filename = ?";
		PreparedStatement ps = conn.prepareStatement(sql);
		ps.setString(1, video.toAbsolutePath().toString());
		ResultSet rs = ps.executeQuery();
		if (!rs.next()) {
			System.err.println("Tried to export a nonexistent video! "+video);
			return;
		}
		int vid = rs.getInt(1);
		ps.close();
		sql = "UPDATE Saved SET exported=? WHERE id=?";
		ps = conn.prepareStatement(sql);
		ps.setInt(1, 1);
		ps.setInt(2, vid);
		ps.execute();
		} catch (SQLException e) {
			System.err.println("Error marking video exported");
			e.printStackTrace();
		}
	}
	
	public void WriteExportManifest(Path output) {
		String sql = "SELECT filename, guid, start, end, machine FROM Recordings INNER JOIN Saved ON Recordings.id = Saved.rid WHERE Saved.exported=0";

		StringBuilder sb = new StringBuilder();
		try {
		PreparedStatement ps = conn.prepareStatement(sql);
		ResultSet res = ps.executeQuery();
		sb.append("filename,guid,start,end,machine\n");
		while (res.next()) {
			sb.append(Paths.get(res.getString(1)).getFileName());
			sb.append(",");
			sb.append(res.getString(2));
			sb.append(",");
			sb.append(res.getTimestamp(3).getTime());
			sb.append(",");
			sb.append(res.getTimestamp(3).getTime());
			sb.append(",");
			sb.append(res.getString(5));
			sb.append("\n");
		}
		res.close();
		} catch (SQLException e) {
			System.err.println("Getting manifest data failed; attempting to continue with partial manifest");
			e.printStackTrace();
		}
 		System.out.println("created manifest");
		System.out.println(sb.toString());
		try {
			Files.writeString(output, sb.toString());
		} catch (IOException e) {
			System.err.println("Failed to write manifest!");
			e.printStackTrace();
		}
		
	}
}
