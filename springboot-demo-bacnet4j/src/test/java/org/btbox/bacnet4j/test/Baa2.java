package org.btbox.bacnet4j.test;

import cn.hutool.core.util.StrUtil;
import com.serotonin.bacnet4j.LocalDevice;
import com.serotonin.bacnet4j.RemoteDevice;
import com.serotonin.bacnet4j.exception.BACnetException;
import com.serotonin.bacnet4j.npdu.ip.IpNetwork;
import com.serotonin.bacnet4j.npdu.ip.IpNetworkBuilder;
import com.serotonin.bacnet4j.transport.DefaultTransport;
import com.serotonin.bacnet4j.type.Encodable;
import com.serotonin.bacnet4j.type.constructed.ObjectPropertyReference;
import com.serotonin.bacnet4j.type.enumerated.PropertyIdentifier;
import com.serotonin.bacnet4j.type.primitive.ObjectIdentifier;
import com.serotonin.bacnet4j.util.*;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.btbox.bacnet4j.utils.IpUtil;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.amqp.RabbitConnectionDetails;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @description:
 * @author: BT-BOX
 * @createDate: 2025/8/14 16:06
 * @version: 1.0
 */
@Slf4j
public class Baa2 {

    private static LocalDevice localDevice;



    @SneakyThrows
    @Test
    public void start() {
        // 获取本地ip地址
        String localAddress = InetAddress.getLocalHost().getHostAddress();

        // 构建 ip network 配置
        IpNetworkBuilder ipNetworkBuilder = new IpNetworkBuilder()
                .withLocalBindAddress(localAddress)
                .withSubnet(localAddress, 24) // Assuming a /24 subnet
                .withPort(47808); // Default BACnet IP port


        IpNetwork network = ipNetworkBuilder.build();

        // 创建本地设备
        localDevice = new LocalDevice(555, new DefaultTransport(network));

        try {
            // Initialize the local device
            localDevice.initialize();
            localDevice.getRemoteDevices();


            // 通过udp广播，获取本网段内的所有设备
            List<RemoteDevice> remoteDevices = getAllRemoteDevices(IpUtil.getBroadcastByIpAndSubnet(localAddress, "255.255.255.255"));

            for (RemoteDevice remoteDevice : remoteDevices) {
                int instanceNumber = remoteDevice.getInstanceNumber();
                RemoteDevice remoteDeviceTemp = getSingleRemoteDevice(instanceNumber);
                // 获取设备的对象列表
                List<ObjectIdentifier> objectListOfDevice = getObjectListOfDevice(remoteDeviceTemp);
                for (ObjectIdentifier objectIdentifier : objectListOfDevice) {
                    // 获取对象的属性列表
                    // 读取属性的值
                    Map<PropertyIdentifier, Encodable> propertyIdentifierEncodableMap = readPropertyValue(remoteDeviceTemp, objectIdentifier);
                }

            }


            // 终止本地虚拟设备
            localDevice.terminate();

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            // Clean up resources
            if (localDevice != null) {
                localDevice.terminate();
            }
            if (network != null) {
                network.terminate();
            }
        }

    }



    private static List<RemoteDevice> getAllRemoteDevices(String networkSegment) throws InterruptedException {
        log.info("************************** 获取本网段所有的设备 " + networkSegment + " ****************************");
        RemoteDeviceDiscoverer remoteDeviceDiscoverer = new RemoteDeviceDiscoverer(localDevice);
        remoteDeviceDiscoverer.start();

        // 等待网段内的设备响应广播消息
        Thread.sleep(3000);

        List<RemoteDevice> remoteDevices = remoteDeviceDiscoverer.getRemoteDevices();
        remoteDevices.forEach(remoteDevice -> {
            try {
                // 下一行的方法为jar包提供的工具方法，所做的内容就是通过 localDevice.send 获取属性值并赋值到remoteDevice上，以便直接通过remoteDevice.get...的方式调用
                DiscoveryUtils.getExtendedDeviceInformation(localDevice, remoteDevice);
            } catch (BACnetException e) {
                log.error("读取本网段所有设备方法出现异常: " + e.getMessage());
                return;
            }

            // 设备id
            int instanceNumber = remoteDevice.getObjectIdentifier().getInstanceNumber();
            // 设备名称
            String deviceName = remoteDevice.getName();
            // 设备型号名称
            String modelName = remoteDevice.getModelName();
            // 对象类型
            String objectType = remoteDevice.getObjectIdentifier().getObjectType().toString();



        });
        remoteDeviceDiscoverer.stop();
        return remoteDevices;
    }

    private static List<ObjectIdentifier> getObjectListOfDevice(RemoteDevice remoteDevice) throws BACnetException {
        System.out.println("************************** 获取设备下的对象列表 " + remoteDevice.getInstanceNumber() + " ****************************");
        List<ObjectIdentifier> objectList = RequestUtils.getObjectList(localDevice, remoteDevice).getValues();
        // 列表中会将设备本身也返回回来，先排除掉吧，毕竟它不是设备下的对象
        List<ObjectIdentifier> realObjectList = objectList.stream().filter(t -> !t.getObjectType().toString().equals("device")).collect(Collectors.toList());
        // realObjectList.forEach(object -> {
        //     try {
        //         Encodable objectName = RequestUtils.readProperty(localDevice,
        //                 remoteDevice,
        //                 new ObjectIdentifier(object.getObjectType(), object.getInstanceNumber()),
        //                 PropertyIdentifier.objectName,
        //                 null);
        //         System.out.println("instanceNumber: " + object.getInstanceNumber() + ", objectType: " + object.getObjectType() + ", objectName: " + objectName);
        //     } catch (BACnetException e) {
        //         e.printStackTrace();
        //     }
        // });
        return realObjectList;
    }


    private static RemoteDevice getSingleRemoteDevice(int remoteDeviceNumber) throws BACnetException {
        log.info("************************** 获取指定的设备 " + remoteDeviceNumber + " ****************************");
        // 还有另一种方法获取指定设备：localDevice.getRemoteDeviceBlocking(remoteDeviceNumber)
        RemoteDevice remoteDeviceTemp = RemoteDeviceFinder.findDevice(localDevice, remoteDeviceNumber).get();
        // 下一行的方法为jar包提供的工具方法，所做的内容就是通过 localDevice.send 获取属性值并赋值到remoteDevice上，以便直接通过remoteDevice.get...的方式调用
        // Tips: 最好保留此方法，因为后续 RequestUtils.readProperty 等方法会使用到此工具方法中对 remoteDevice 这个对象上所赋的值（注释此行则可以看到NullPointerException的报错）
        DiscoveryUtils.getExtendedDeviceInformation(localDevice, remoteDeviceTemp);

        System.out.println("remoteDeviceTemp.getName() = " + remoteDeviceTemp.getName());
        System.out.println("remoteDeviceTemp.getModelName() = " + remoteDeviceTemp.getModelName());
        System.out.println("objectType: " + remoteDeviceTemp.getObjectIdentifier().getObjectType().toString() + ", instanceNumber: " + remoteDeviceTemp.getObjectIdentifier().getInstanceNumber());
        return remoteDeviceTemp;
    }

    /**
     * 获取对象属性
     * @param remoteDevice
     * @param objectIdentifier
     * @return
     * @throws BACnetException
     */
    private static Map<PropertyIdentifier, Encodable> readPropertyValue(RemoteDevice remoteDevice, ObjectIdentifier objectIdentifier) throws BACnetException {

        // 单独查询指定的属性值
        String objectName = RequestUtils.getProperty(localDevice, remoteDevice, objectIdentifier, PropertyIdentifier.objectName).toString();

        System.out.println("************************* 属性对象名: " + objectName + " *******************************");
        PropertyIdentifier[] propertyIdentifiers = new PropertyIdentifier[]{
                PropertyIdentifier.objectType,
                PropertyIdentifier.objectName,
                PropertyIdentifier.description,
                // presentValue是该对象当前的值，其它属性都是描述这个对象的
                PropertyIdentifier.presentValue,
                PropertyIdentifier.eventState,
                PropertyIdentifier.outOfService,
                PropertyIdentifier.statusFlags,
                PropertyIdentifier.updateInterval,
                PropertyIdentifier.timeDelay,
                PropertyIdentifier.reliability
        };

        // PropertyReferences propertyReferences = new PropertyReferences();
        // propertyReferences.add(objectIdentifier, propertyIdentifiers);
        // PropertyValues objectPropertyReferences = RequestUtils.readProperties(localDevice, remoteDevice, propertyReferences, true, null);
        // for (ObjectPropertyReference objectPropertyReference : objectPropertyReferences) {
        //     String value = objectPropertyReferences.getString(objectPropertyReference.getObjectIdentifier(), objectPropertyReference.getPropertyIdentifier());
        //     System.out.println(objectPropertyReference.getPropertyIdentifier().toString() + ": " + value);
        // }


        // 方法三：RequestUtils.getProperties， 传入的是已经获取到的remoteDevice对象，不可以指定是否允许为null，返回的是 Map<PropertyIdentifier, Encodable>
        Map<PropertyIdentifier, Encodable> properties = RequestUtils.getProperties(localDevice, remoteDevice, objectIdentifier, null, propertyIdentifiers);

        properties.forEach((propertyIdentifier, value) -> {
            System.out.println(propertyIdentifier.toString() + ": " + checkPropertyErrorClassOrNullSetNull(value.toString()));
        });

        return properties;
    }

    private static String checkPropertyErrorClassOrNullSetNull(String propertyName) {
        String Null = "Null";
        String errorClass = "errorClass";
        if (StrUtil.startWith(propertyName, errorClass) || propertyName.equals(Null)) {
            return null;
        }
        return propertyName;
    }


}