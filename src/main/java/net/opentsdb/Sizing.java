// This file is part of OpenTSDB.
// Copyright (C) 2017  The OpenTSDB Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package net.opentsdb;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Future;

import org.openjdk.jol.info.ClassLayout;
import org.openjdk.jol.vm.VM;

import com.google.common.util.concurrent.ListenableFuture;
import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;

public class Sizing {

  public static void main(final String[] args) {
    System.out.println(VM.current().details());
    
    System.out.println(ClassLayout.parseClass(byte.class).toPrintable());
    System.out.println(ClassLayout.parseClass(short.class).toPrintable());
    System.out.println(ClassLayout.parseClass(int.class).toPrintable());
    System.out.println(ClassLayout.parseClass(long.class).toPrintable());
    
    //System.out.println(ClassLayout.parseClass(Future.class).toPrintable());
    //System.out.println(ClassLayout.parseClass(ListenableFuture.class).toPrintable());
//    System.out.println(ClassLayout.parseClass(Deferred.class).toPrintable());
//    System.out.println(ClassLayout.parseClass(Callback.class).toPrintable());
//    System.out.println(ClassLayout.parseClass(CompletableFuture.class).toPrintable());
//    System.out.println(ClassLayout.parseClass(CompletionStage.class).toPrintable());
    
    System.out.println("********** ZONED DATE TIME ************");
    System.out.println(ClassLayout.parseClass(ZonedDateTime.class).toPrintable());
    
    ZonedDateTime zdt = ZonedDateTime.ofInstant(Instant.ofEpochMilli(System.currentTimeMillis()), ZoneId.of("UTC"));
    System.out.println(ClassLayout.parseInstance(zdt).toPrintable());
    if (true) {
      return;
    }
    final Deferred<Object> deferred = new Deferred<Object>();
    final CompletableFuture<Object> cf = new CompletableFuture<Object>();
    
    System.out.println(ClassLayout.parseInstance(deferred).toPrintable());
    System.out.println(ClassLayout.parseInstance(cf).toPrintable());
   
    
    class MyCB implements Callback <Object, Object> {
      @Override
      public Object call(Object arg) throws Exception {
        return null;
      }
    }
    System.out.println(ClassLayout.parseInstance(new MyCB()).toPrintable());
    
    
  }
}
