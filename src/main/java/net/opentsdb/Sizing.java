package net.opentsdb;

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
