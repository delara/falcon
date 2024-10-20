package com.edgestream.worker.testing;


import java.io.*;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;


public class CPUInfo {

    public static void main(String[] args) throws IOException {

        //parseWindowsCpuInfo();
        parseLinuxCpuInfo();

    }


    static void parseWindowsCpuInfo() throws IOException {

        ProcessBuilder pb = new ProcessBuilder("wmic", "cpu", "get" , "NAME");

        //pb.redirectOutput(ProcessBuilder.Redirect.INHERIT);
        pb.redirectError(ProcessBuilder.Redirect.INHERIT);
        Process p = pb.start();


        BufferedReader reader = new BufferedReader(new InputStreamReader(p.getInputStream()));
        StringBuilder builder = new StringBuilder();
        String line = null;
        while ( (line = reader.readLine()) != null) {
            if (!line.startsWith("Name") && !line.startsWith(" ")  ){
                builder.append(line);
            }

        }
        String result = builder.toString();

        System.out.println(result);

    }


    static void parseLinuxCpuInfo() throws IOException {

        String sampleCPU = "Architecture:        x86_64\n" +
                "CPU op-mode(s):      32-bit, 64-bit\n" +
                "Byte Order:          Little Endian\n" +
                "Address sizes:       48 bits physical, 48 bits virtual\n" +
                "CPU(s):              2\n" +
                "On-line CPU(s) list: 0,1\n" +
                "Thread(s) per core:  1\n" +
                "Core(s) per socket:  2\n" +
                "Socket(s):           1\n" +
                "NUMA node(s):        1\n" +
                "Vendor ID:           AuthenticAMD\n" +
                "CPU family:          23\n" +
                "Model:               113\n" +
                "Model name:          AMD Ryzen 9 3900X 12-Core Processor\n" +
                "Stepping:            0\n" +
                "CPU MHz:             3792.874\n" +
                "BogoMIPS:            7585.74\n" +
                "Hypervisor vendor:   KVM\n" +
                "Virtualization type: full\n" +
                "L1d cache:           32K\n" +
                "L1i cache:           32K\n" +
                "L2 cache:            512K\n" +
                "L3 cache:            65536K\n" +
                "NUMA node0 CPU(s):   0,1\n" +
                "Flags:               fpu vme de pse tsc msr pae mce cx8 apic sep mtrr pge mca cmov pat pse36 clflush mmx fxsr sse sse2 ht syscall nx mmxext fxsr_opt rdtscp lm constant_tsc rep_good nopl nonstop_tsc cpuid extd_apicid tsc_known_freq pni pclmulqdq ssse3 cx16 sse4_1 sse4_2 x2apic movbe popcnt aes xsave avx rdrand hypervisor lahf_lm cmp_legacy cr8_legacy abm sse4a misalignsse 3dnowprefetch ssbd vmmcall fsgsbase avx2 rdseed clflushopt arat"
                ;

        //ProcessBuilder pb = new ProcessBuilder("lscpu");

        //pb.redirectOutput(ProcessBuilder.Redirect.INHERIT);
        //pb.redirectError(ProcessBuilder.Redirect.INHERIT);
        //Process p = pb.start();

        InputStream inputStream = new ByteArrayInputStream(sampleCPU.getBytes(StandardCharsets.UTF_8));


        BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
        StringBuilder builder = new StringBuilder();
        String line = null;
        while ( (line = reader.readLine()) != null) {

            if (line.startsWith("Model name")){
                //System.out.println(line);
                builder.append(line);
            }

        }

        System.out.println(builder.toString().split(":")[1].trim());

    }

}




