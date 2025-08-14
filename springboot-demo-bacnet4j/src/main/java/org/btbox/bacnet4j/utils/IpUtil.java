package org.btbox.bacnet4j.utils;

import java.net.*;
import java.util.Enumeration;

/**
 * @ClassName: IpUtil
 * @Description: TODO-wcy
 * @Author: wcy
 * @Date: 2022/4/28 16:32
 * @Version: 1.0
 */
public class IpUtil {
    public static String getNetworkSegmentByIpAndSubnet(String ip, String subnet) {
        long networkSegment = ipToLong(ip) & ipToLong(subnet);
        return longToIp(networkSegment);
    }

    public static String getBroadcastByIpAndSubnet(String ip, String subnet) {
        long broadcast = ipToLong(ip) | (~ipToLong(subnet));
        return longToIp(broadcast);
    }

    public static long ipToLong(String ipAddress) {
        long result = 0;
        String[] ipAddressInArray = ipAddress.split("\\.");
        for (int i = 3; i >= 0; i--) {
            long ip = Long.parseLong(ipAddressInArray[3 - i]);
            result |= ip << (i * 8);
        }
        return result;
    }

    public static String longToIp(long ip) {
        return ((ip >> 24) & 0xFF) + "."
                + ((ip >> 16) & 0xFF) + "."
                + ((ip >> 8) & 0xFF) + "."
                + (ip & 0xFF);
    }

    /**
     * 获取本机IP地址
     * @return
     * @throws SocketException
     */
    public static String getIpAddress() throws SocketException {
        String ipString = null;
        Inet4Address inet4Address  = getInet4Address();
        if(inet4Address != null){
				/*NetworkInterface networkInterface = NetworkInterface.getByInetAddress(inet4Address);
	    		for (InterfaceAddress address : networkInterface.getInterfaceAddresses()) {
	    			ipString = address.getAddress().getHostAddress();
	    		}*/
            ipString = inet4Address.getHostAddress();
        }
        return ipString;
    }

    /**
     * 获取网络前缀长度，
     * 如果长度为8，则表示掩码是255.0.0.0，
     * 如果长度为16，则表示掩码是255.255.0.0，
     * 如果长度为24，则表示掩码是255.255.255.0，
     * @return
     * @throws UnknownHostException
     * @throws SocketException
     */
    public static int getNetworkPrefixLength() throws UnknownHostException, SocketException{
        int networkPrefixLength = 0;
        Inet4Address inet4Address  = getInet4Address();
        if(inet4Address != null){
            NetworkInterface networkInterface = NetworkInterface.getByInetAddress(inet4Address);
            for (InterfaceAddress address : networkInterface.getInterfaceAddresses()) {
                if(address.getAddress() instanceof Inet4Address){
                    networkPrefixLength =  address.getNetworkPrefixLength();
                }

            }
        }
        return networkPrefixLength;
    }

    /**
     * 获取网络掩码255.0.0.0，255.0.0.0，255.0.0.0，
     * @return
     * @throws UnknownHostException
     * @throws SocketException
     */
    public static String getSubnet() throws UnknownHostException, SocketException{
        String subnet = null;
        int prefix = getNetworkPrefixLength();
        if(prefix > 0){
            if(prefix == 8){
                subnet = "255.0.0.0";
            }else if(prefix == 16){
                subnet = "255.255.0.0";
            }else if(prefix == 24){
                subnet = "255.255.255.0";
            }else if(prefix == 32){
                subnet = "255.255.255.255";
            }
        }
        return subnet;
    }

    private static Inet4Address getInet4Address() throws SocketException {
        Inet4Address inet4Address = null;
        Enumeration<NetworkInterface> allNetInterfaces = NetworkInterface.getNetworkInterfaces();
        InetAddress ip = null;
        while (allNetInterfaces.hasMoreElements()) {
            NetworkInterface netInterface = (NetworkInterface) allNetInterfaces.nextElement();
            //用于排除回送接口,非虚拟网卡,未在使用中的网络接口.
            if (netInterface.isLoopback() || netInterface.isVirtual() || !netInterface.isUp()) {
                continue;
            } else {
                Enumeration<InetAddress> addresses = netInterface.getInetAddresses();
                while (addresses.hasMoreElements()) {
                    ip = addresses.nextElement();
                    if (ip != null && ip instanceof Inet4Address) {
                        inet4Address = (Inet4Address)ip;
                        break;
                    }
                }
                if(inet4Address != null){
                    break;
                }
            }
        }
        return inet4Address;
    }

}
