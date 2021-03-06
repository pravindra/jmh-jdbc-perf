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

import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

import cdjd.com.google.common.io.ByteStreams;

public class MyBenchmark {
  private static final String DB_DRIVER = "com.dremio.jdbc.Driver";
  private static final String DB_CONNECTION = "jdbc:dremio:direct=localhost:31010";
  private static final String DB_USER = "x";
  private static final String DB_PASSWORD = "dremio123";
  static final int MAX_VALUE = 500000000;

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

  private void printResults(ResultSet rs) throws SQLException {
    ResultSetMetaData rsMeta = rs.getMetaData();
    int numColumns = rsMeta.getColumnCount();
    while (rs.next()) {
      for (int i = 1; i <= numColumns; i++) {
        if (i > 1) {
          System.out.print(",  ");
        }

        String columnValue = rs.getString(i);
        System.out.print(columnValue + " " + rsMeta.getColumnName(i));
      }
      System.out.println("");
    }
  }

  /*@Benchmark
  @Fork(1)
  @Warmup(iterations = 5)
  @Measurement(iterations = 3)
  @BenchmarkMode(Mode.SampleTime)
  public void testSimple(DBState state) {
    try {
      String query = "SELECT count(x+N2x+N3x) as mycount FROM json.d500";
      ResultSet rs = state.statement.executeQuery(query);
      printResults(rs);
    } catch (SQLException e) {
      e.printStackTrace();
    }
  }

  @Benchmark
  @Fork(1)
  @Warmup(iterations = 5)
  @Measurement(iterations = 3)
  @BenchmarkMode(Mode.SampleTime)
  public void testSimple5(DBState state) {
    try {
      String query = "SELECT " +
        "sum(x + N2x + N3x), " +
        "sum(x * N2x - N3x), " +
        "sum(3 * x + 2 * N2x + N3x), " +
        "count(x >= N2x - N3x), " +
        "count(x + N2x = N3x)" +
        "FROM json.d500";
      ResultSet rs = state.statement.executeQuery(query);
      printResults(rs);
    } catch (SQLException e) {
      e.printStackTrace();
    }
  }*/

  /*
  @Benchmark
  @Fork(1)
  @Warmup(iterations = 5)
  @Measurement(iterations = 3)
  @BenchmarkMode(Mode.SampleTime)
  public void testSimple5(DBState state) {
    try {
      String query = "SELECT " +
        "sum(x + N2x + N3x), " +
        "sum(x * N2x - N3x), " +
        "sum(3 * x + 2 * N2x + N3x), " +
        "count(x >= N2x - N3x), " +
        "count(x + N2x = N3x), " +
        "sum(x - N2x + N3x), " +
        "sum(x * N2x + N3x), " +
        "sum(x + 2 * N2x + 3 * N3x), " +
        "count(x <= N2x - N3x), " +
        "count(x = N3x - N2x)" +
        "FROM json.d500";
      ResultSet rs = state.statement.executeQuery(query);
      printResults(rs);
    } catch (SQLException e) {
      e.printStackTrace();
    }
  }
  */

  @Benchmark
  @Fork(1)
  @Warmup(iterations = 5)
  @Measurement(iterations = 3)
  @BenchmarkMode(Mode.SampleTime)
  public void testBigOne(DBState state) {
    try {
      InputStream in = getClass().getResourceAsStream("/sample.sql");
      String query = new String(ByteStreams.toByteArray(in));

      //System.out.println(query);
      ResultSet rs = state.statement.executeQuery(query);
      printResults(rs);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  /*
  @Benchmark
  @Fork(1)
  @Warmup(iterations = 5)
  @Measurement(iterations = 3)
  @BenchmarkMode(Mode.SampleTime)
  public void testCase10(DBState state) {
    try {
      String query = "SELECT " + buildCaseExpr(10, "x") + " FROM json.d500";
      //System.out.println(query);
      ResultSet rs = state.statement.executeQuery(query);
      printResults(rs);
    } catch (SQLException e) {
      e.printStackTrace();
    }
  }
  */

  /*
  @Benchmark
  @Fork(1)
  @Warmup(iterations = 5)
  @Measurement(iterations = 3)
  @BenchmarkMode(Mode.SampleTime)
  public void testCase100(DBState state) {
    try {
      String query = "SELECT " +
        buildCaseExpr(100, "x") + ", " +
        buildCaseExpr(100, "N2x") + ", " +
        buildCaseExpr(100, "N3x") +
        " FROM json.d500";
      //System.out.println(query);

      ResultSet rs = state.statement.executeQuery(query);
      printResults(rs);
    } catch (SQLException e) {
      e.printStackTrace();
    }
  }
  */
}
