/*
 * Copyright (c) 2014, Oracle America, Inc.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *  * Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer.
 *
 *  * Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 *
 *  * Neither the name of Oracle nor the names of its contributors may be used
 *    to endorse or promote products derived from this software without
 *    specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF
 * THE POSSIBILITY OF SUCH DAMAGE.
 */

package com.example;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;

public class MyBenchmark {
  private static final String DB_DRIVER = "com.dremio.jdbc.Driver";
  private static final String DB_CONNECTION = "jdbc:dremio:direct=localhost:31010";
  private static final String DB_USER = "x";
  private static final String DB_PASSWORD = "dremio123";

  @State(Scope.Thread)
  public static class DBState {
    private Connection dbConnection;
    public Statement statement;

    @Setup(Level.Trial)
    public void setup() {
      try {
        Class.forName(DB_DRIVER);
        dbConnection = DriverManager.getConnection(DB_CONNECTION, DB_USER, DB_PASSWORD);
        statement = dbConnection.createStatement();
      } catch (Exception e) {
        System.out.println(e.getMessage());
        e.printStackTrace();
      }
    }

    @TearDown(Level.Trial)
    public void teardown() {
      try {
        dbConnection.close();
      } catch (SQLException e) {
        System.out.println(e.getMessage());
        e.printStackTrace();
      }
    }
  }

  @Benchmark
  public void testMethod(DBState state) {
    try {
      String query = "SELECT count(x+N2x+N3x) as mycount FROM json.\"10M.json\"";
      ResultSet rs = state.statement.executeQuery(query);

      // Let's iterate through the java ResultSet
      while (rs.next()) {
        long count = rs.getLong("mycount");

        // Simply Print the results
        System.out.println("mycount " + count);
      }
    } catch (SQLException e) {
      e.printStackTrace();
    }
  }

}
