package net.opentsdb;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.CompilerControl;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.infra.Blackhole;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;

/**
 * -Dlogback.configurationFile=logback.xml -XX:+UnlockDiagnosticVMOptions -XX:LogFile=jvm.log -XX:+PrintInlining -XX:+TraceClassLoading -XX:+PrintAssembly -XX:+LogCompilation -XX:+DebugNonSafepoints -XX:CompileThreshold=20
 *
 */
public class LogOptimization {
  static final Logger LOG = LoggerFactory.getLogger(LogOptimization.class);

  @State(Scope.Thread)
  public static class Context {
    int iterations = 1024 * 1024;
    Random rnd = new Random(System.currentTimeMillis());

    String bigToStringMethod() {
      StringBuilder buf = new StringBuilder();
      for (int i = 0; i < 16; i++) {
        buf.append(rnd.nextDouble());
      }
      return buf.toString();
    }
  }

  @Benchmark
  @CompilerControl(CompilerControl.Mode.DONT_INLINE)
  public static void stringConcatDoubles(Context context, Blackhole blackHole) {
    for (int i = 0; i < 1024 * 1024 * 16; i++) {
      double num1 = context.rnd.nextDouble();
      double num2 = context.rnd.nextDouble();
      double num3 = context.rnd.nextDouble();

      LOG.debug("Testing " + num1 + " " + num2 + " " + num3);

      blackHole.consume(num1);
      blackHole.consume(num2);
      blackHole.consume(num3);
    }
  }

  @Benchmark
  @CompilerControl(CompilerControl.Mode.DONT_INLINE)
  public static void stringConcatMethods(Context context, Blackhole blackHole) {
    for (int i = 0; i < 1024 * 1024 * 16; i++) {
      double num1 = context.rnd.nextDouble();
      double num2 = context.rnd.nextDouble();
      double num3 = context.rnd.nextDouble();

      LOG.debug("Testing "
          + context.bigToStringMethod() + " "
          + context.bigToStringMethod() + " "
          + context.bigToStringMethod());

      blackHole.consume(num1);
      blackHole.consume(num2);
      blackHole.consume(num3);
    }
  }

  @Benchmark
  @CompilerControl(CompilerControl.Mode.DONT_INLINE)
  public static void stringParamDoubles(Context context, Blackhole blackHole) {
    for (int i = 0; i < 1024 * 1024 * 16; i++) {
      double num1 = context.rnd.nextDouble();
      double num2 = context.rnd.nextDouble();
      double num3 = context.rnd.nextDouble();

      LOG.debug("Testing {} {} {}", num1, num2, num3);

      blackHole.consume(num1);
      blackHole.consume(num2);
      blackHole.consume(num3);
    }
  }

  /**
   * 5th compilation of the method at C2/L4. The important bits about checking for the debug logging have been
   * inlined. HOWEVER, the 3 calls to context.bigToStringMethod() have _not_ been inlined and are executed
   * per the 'callq' ops.
   *
   * At address 0x000000011f4461d3, there is a compare call that checks to see if logging is enabled period.
   * This is the FIRST of the uncommon traps. If the "jne" below the compare jumps, it will land at L000b that
   * walks down to 0x000000011f44639f which is a call back to the bytecode to execute the uncommon path.
   *
   * If logging _is_ enabled, it checks more flags in Log4J and eventually winds up checking the debug filter.
   * Each of these is trapped similarly in that if any of those are toggled other than usual, the trap is sprung
   * and execution returns to the byte code.
   *
   * Finally, at 0x000000011f446223 we have the last Log4J check on the debug filter. If debugging is disabled
   * then the trap is valid and at 0x000000011f446231 it has skipped the code that would create the log line and
   * is now onto the BlackHole call.
   *
   * SO, the gist is that the optimized method will _still_ call toString() methods and push the results into
   * registers even when the actual logging bit is optimized out.
   *
   * ................... truncated
   * 0x000000011f44619b: callq 0x000000011781cd80  ; ImmutableOopMap{[0]=Oop [8]=Oop }
   *                                               ;*invokevirtual bigToStringMethod {reexecute=0 rethrow=0 return_oop=1}
   *                                               ; - net.opentsdb.LogOptimization::stringParamMethods@46 (line 85)
   *                                               ;   {optimized virtual_call}
   * 0x000000011f4461a0: mov %rax,0x38(%rsp)  ;*aastore {reexecute=0 rethrow=0 return_oop=0}
   *                                          ; - net.opentsdb.LogOptimization::stringParamMethods@49 (line 85)
   * 0x000000011f4461a5: mov (%rsp),%rsi
   * 0x000000011f4461a9: xchg %ax,%ax
   *
   * ................... 2 MORE EXECUTIONS of the bigToStringMethod
   *
   * 0x000000011f4461c0: movabs $0x70099cbd0,%r10  ;   {oop(a 'org/apache/logging/slf4j/Log4jLogger'{0x000000070099cbd0})}
   * 0x000000011f4461ca: mov 0x10(%r10),%r11d  ;*getfield logger {reexecute=0 rethrow=0 return_oop=0}
   *                                           ; - org.apache.logging.slf4j.Log4jLogger::debug@1 (line 134)
   *                                           ; - net.opentsdb.LogOptimization::stringParamMethods@64 (line 84)
   * 0x000000011f4461ce: mov 0x8(%r12,%r11,8),%r10d  ; implicit exception: dispatches to 0x000000011f4467d8
   * 0x000000011f4461d3: cmp $0x15ccc0,%r10d  ;   {metadata('org/apache/logging/log4j/core/Logger')}
   * 0x000000011f4461da: jne L000b  ;*synchronization entry
   *                                ; - org.apache.logging.log4j.core.Logger::isEnabled@-1 (line 180)
   *                                ; - org.apache.logging.log4j.spi.AbstractLogger::logIfEnabled@7 (line 1890)
   *                                ; - org.apache.logging.slf4j.Log4jLogger::debug@13 (line 134)
   *                                ; - net.opentsdb.LogOptimization::stringParamMethods@64 (line 84)
   * 0x000000011f4461e0: lea (%r12,%r11,8),%r9  ;*invokeinterface logIfEnabled {reexecute=0 rethrow=0 return_oop=0}
   *                                            ; - org.apache.logging.slf4j.Log4jLogger::debug@13 (line 134)
   *                                            ; - net.opentsdb.LogOptimization::stringParamMethods@64 (line 84)
   *
   * .......... MORE LOG4J enabled checks then...
   *
   * 0x000000011f44621f: mov 0xc(%r11),%r8d  ;*getfield intLevel {reexecute=0 rethrow=0 return_oop=0}
   *                                         ; - org.apache.logging.log4j.Level::intLevel@1 (line 133)
   *                                         ; - org.apache.logging.log4j.core.Logger$PrivateConfig::filter@65 (line 463)
   *                                         ; - org.apache.logging.log4j.core.Logger::isEnabled@9 (line 180)
   *                                         ; - org.apache.logging.log4j.spi.AbstractLogger::logIfEnabled@7 (line 1890)
   *                                         ; - org.apache.logging.slf4j.Log4jLogger::debug@13 (line 134)
   *                                         ; - net.opentsdb.LogOptimization::stringParamMethods@64 (line 84)
   * 0x000000011f446223: cmp %r8d,%r10d
   * 0x000000011f446226: jge L0014  ;*if_icmplt {reexecute=0 rethrow=0 return_oop=0}
   *                                ; - org.apache.logging.log4j.core.Logger$PrivateConfig::filter@68 (line 463)
   *                                ; - org.apache.logging.log4j.core.Logger::isEnabled@9 (line 180)
   *                                ; - org.apache.logging.log4j.spi.AbstractLogger::logIfEnabled@7 (line 1890)
   *                                ; - org.apache.logging.slf4j.Log4jLogger::debug@13 (line 134)
   *                                ; - net.opentsdb.LogOptimization::stringParamMethods@64 (line 84)
   * 0x000000011f44622c: mov 0x8(%rsp),%r8
   * 0x000000011f446231: vmovsd 0x98(%r8),%xmm0  ;*getfield d1 {reexecute=0 rethrow=0 return_oop=0}
   *                                             ; - org.openjdk.jmh.infra.Blackhole::consume@1 (line 420)
   *                                             ; - net.opentsdb.LogOptimization::stringParamMethods@71 (line 89)
   *
   *............ TRUNCATED example of the trap return call
   *
   * 0x000000011f44639f: callq 0x000000011781be00  ; ImmutableOopMap{rbp=Oop [0]=Oop [12]=NarrowOop [40]=Oop [56]=Oop [64]=Oop }
   *                                               ;*invokeinterface logIfEnabled {reexecute=0 rethrow=0 return_oop=0}
   *                                               ; - org.apache.logging.slf4j.Log4jLogger::debug@13 (line 134)
   *                                               ; - net.opentsdb.LogOptimization::stringParamMethods@64 (line 84)
   *                                               ;   {runtime_call UncommonTrapBlob}
   *
   * @param context
   * @param blackHole
   */
  @Benchmark
  @CompilerControl(CompilerControl.Mode.DONT_INLINE)
  public static void stringParamMethods(Context context, Blackhole blackHole) {
    for (int i = 0; i < 1024 * 1024 * 16; i++) {
      double num1 = context.rnd.nextDouble();
      double num2 = context.rnd.nextDouble();
      double num3 = context.rnd.nextDouble();

      LOG.debug("Testing {} {} {}",
          context.bigToStringMethod(),
          context.bigToStringMethod(),
          context.bigToStringMethod());

      blackHole.consume(num1);
      blackHole.consume(num2);
      blackHole.consume(num3);
    }
  }

  @Benchmark
  @CompilerControl(CompilerControl.Mode.DONT_INLINE)
  public static void debugCheckDoubles(Context context, Blackhole blackHole) {
    for (int i = 0; i < 1024 * 1024 * 16; i++) {
      double num1 = context.rnd.nextDouble();
      double num2 = context.rnd.nextDouble();
      double num3 = context.rnd.nextDouble();

      if (LOG.isDebugEnabled()) {
        LOG.debug("Testing {} {} {}", num1, num2, num3);
      }

      blackHole.consume(num1);
      blackHole.consume(num2);
      blackHole.consume(num3);
    }
  }

  @Benchmark
  @CompilerControl(CompilerControl.Mode.DONT_INLINE)
  public static void debugCheckMethods(Context context, Blackhole blackHole) {
    for (int i = 0; i < 1024 * 1024 * 16; i++) {
      double num1 = context.rnd.nextDouble();
      double num2 = context.rnd.nextDouble();
      double num3 = context.rnd.nextDouble();

      if (LOG.isDebugEnabled()) {
        LOG.debug("Testing {} {} {}",
            context.bigToStringMethod(),
            context.bigToStringMethod(),
            context.bigToStringMethod());
      }

      blackHole.consume(num1);
      blackHole.consume(num2);
      blackHole.consume(num3);
    }
  }
}
