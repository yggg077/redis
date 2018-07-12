package redis.learning;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.Arrays;
import java.util.Scanner;

import org.apache.commons.lang.StringUtils;

public class MyRedisClient {
    //redis ip
    private final static String IP = "127.0.0.1";
    //redis 端口
    private final static int PORT = 6379;
    //redis 接收数据缓冲区大小
    private final static int REPLY_BUFF = 256;

    //main方法，接收控制台的命令，发送给redis
    public static void main(String[] args) {
        Scanner in = new Scanner(System.in);
        while(in.hasNext()){
            String cmd = in.nextLine();
            if(StringUtils.isNotEmpty(cmd)){
                //输入exit时退出
                if("exit".equals(cmd)){
                    break;
                }
                String reply = sendCmdToRedis(cmd);
                System.out.println(reply);
            }
        }
        in.close();
    }
    
    //向redis发送命令
    private static String sendCmdToRedis(String cmd){
        if(StringUtils.isEmpty(cmd)){
            return null;
        }
        Socket socket = null;
        OutputStream out = null;
        InputStream in = null;
        try{
            //根据redis协议，构造命令的byte数组
            byte[] cmdBytes = cmd(cmd);
            //socket连接
            socket = new Socket(IP, PORT);
            //获取socket输出流
            out = socket.getOutputStream();
            //获取socket输入流
            in = socket.getInputStream();
            //发送命令
            out.write(cmdBytes);
            //接收缓存
            byte[] buff = new byte[REPLY_BUFF];
            //读取redis回复
            if(in.read(buff) > 0){
                return handleReply(buff);
            }
        }catch(Exception e){
            e.printStackTrace();
        }finally{
            if(socket != null){
                try{
                    out.close();
                    in.close();
                    socket.close();
                }catch(Exception e){}
            }
        }
        return null;
    }

    //根据redis协议，构造命令的byte数组
    private static byte[] cmd(String cmd){
        if(StringUtils.isEmpty(cmd)){
            return null;
        }
        StringBuilder sb = new StringBuilder();
        String[] cmds = cmd.trim().split(" ");
        sb.append("*" + cmds.length);
        sb.append("\r\n");
        int i = 0;
        while(i < cmds.length){
            String c = cmds[i++];
            sb.append("$" + c.length());
            sb.append("\r\n");
            sb.append(c);
            sb.append("\r\n");
        }
        return sb.toString().getBytes();
    }
    
    //处理redis回复
    private static String handleReply(byte[] reply) {
        if(reply == null || reply.length == 0){
            return null;
        }
        byte first = reply[0];
        if(first == '+'){
            //单行回复
            int end = findRN(reply, 1);
            return new String(reply, 1, end - 1);
        }else if(first == '-'){
            //错误消息
            int end = findRN(reply, 1);
            return new String(reply, 1, end - 1);
        }else if(first == '*'){
            //多个批量回复
            int end = findRN(reply, 1);
            int replyLength = getInt(reply, 1, end);
            String[] rs = new String[replyLength];
            for(int i = 0; i < replyLength; i++){
                int dollar = findDollar(reply, end);
                end = findRN(reply, dollar);
                int length = getInt(reply, dollar + 1, end);
                rs[i] = length == -1 ? "-1" : new String(reply, end + 2, length);
            }
            return Arrays.toString(rs);
        }else if(first == '$'){
            //批量回复
            int end = findRN(reply, 1);
            int length = getInt(reply, 1, end);
            return length == -1 ? "-1" : new String(reply, end + 2, length);
        }else if(first == ':'){
            //整型数字
            int end = findRN(reply, 1);
            int rs = getInt(reply, 1, end);
            return "(integer)" + rs;
        }
        return null;
    }
    
    //获取bytes指定区间的整数
    private static int getInt(byte[] bytes, int start, int end) {
        if(bytes == null || bytes.length == 0 || start > end || end >= bytes.length){
            return -1;
        }
        int rs = 0;
        boolean isNeg = false;
        if(bytes[start] == '-'){
            isNeg = true;
            start++;
        }
        while(start < end){
            rs = rs * 10 + bytes[start] - '0';
            start++;
        }
        return isNeg ? -rs : rs;
    }

    //在byte数组中，从指定位置开始查找 \r\n 的位置
    private static int findRN(byte[] bytes, int start){
        if(bytes == null || bytes.length == 0 || start >= bytes.length){
            return -1;
        }
        start = Math.max(start, 0);
        boolean find = false;
        while(start < bytes.length - 1){
            if(bytes[start] == '\r'){
                if(bytes[start + 1] != '\n'){
                    throw new RuntimeException("不合规的回复");
                }
                find = true;
                break;
            }
            start++;
        }
        return find ? start : -1;
    }
    
    //在byte数组中，从指定位置开始查找 $ 的位置
    private static int findDollar(byte[] bytes, int start){
        if(bytes == null || bytes.length == 0 || start >= bytes.length){
            return -1;
        }
        start = Math.max(start, 0);
        boolean find = false;
        while(start < bytes.length - 1){
            if(bytes[start] == '$'){
                find = true;
                break;
            }
            start++;
        }
        return find ? start : -1;
    }
}
