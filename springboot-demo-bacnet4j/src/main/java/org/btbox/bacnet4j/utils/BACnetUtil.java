// package org.btbox.bacnet4j.utils;
//
// import com.serotonin.bacnet4j.LocalDevice;
// import com.serotonin.bacnet4j.RemoteDevice;
// import com.serotonin.bacnet4j.exception.BACnetException;
// import com.serotonin.bacnet4j.npdu.ip.IpNetwork;
// import com.serotonin.bacnet4j.npdu.ip.IpNetworkBuilder;
// import com.serotonin.bacnet4j.transport.DefaultTransport;
// import com.serotonin.bacnet4j.type.Encodable;
// import com.serotonin.bacnet4j.type.enumerated.PropertyIdentifier;
// import com.serotonin.bacnet4j.type.primitive.ObjectIdentifier;
// import com.serotonin.bacnet4j.util.*;
//
// import java.util.List;
// import java.util.Map;
// import java.util.stream.Collectors;
//
//
// public class BACnetUtil {
//
//     public static LocalDevice initLocalDevice(IpNetwork ipNetwork, int localDeviceNumber) throws Exception {
//         System.out.println("************************** 初始化本地虚拟设备 " + localDeviceNumber + " ****************************");
//         LocalDevice localDevice = new LocalDevice(localDeviceNumber, new DefaultTransport(ipNetwork));
//         localDevice.initialize();
//         localDevice.startRemoteDeviceDiscovery();
//         return localDevice;
//     }
//
//     public static IpNetwork initIpNetwork(String ip, String subnet) {
//         System.out.println("************************** 初始化网络信息 ****************************");
//         IpNetworkBuilder ipNetworkBuilder = new IpNetworkBuilder()
//                 .withLocalBindAddress(ip)
//                 .withSubnet(subnet, 24)
//                 //Yabe默认的UDP端口
//                 .withPort(47808);
//                 // .withBroadcast(IpUtil.getBroadcastByIpAndSubnet(ip, subnet), 24)
//                 // .withReuseAddress(true);
//
//         System.out.println("ip:        " + ip);
//         System.out.println("subnet:    " + subnet);
//         System.out.println("broadcast: " + IpUtil.getBroadcastByIpAndSubnet(ip, subnet));
//         String networkSegment = IpUtil.getNetworkSegmentByIpAndSubnet(ip, subnet);
//         System.out.println("所在网段:    " + networkSegment);
//
//         return ipNetworkBuilder.build();
//     }
//
//
//     private static Encodable writePropertyValue(RemoteDevice remoteDevice, ObjectIdentifier objectIdentifier, PropertyIdentifier propertyIdentifier, Encodable valueToWrite) throws BACnetException, InterruptedException {
//         System.out.println("************************** 修改对象中属性的值 对象id: " + objectIdentifier.getInstanceNumber() + ", 属性名: " + propertyIdentifier.toString() + " ****************************");
//
//         Encodable propertyValueBeforeWrite = RequestUtils.getProperty(localDevice, remoteDevice, objectIdentifier, propertyIdentifier);
//         System.out.println("propertyValueBeforeWrite = " + propertyValueBeforeWrite);
//         /*// 原文备注说：必须先修改out of service为true，但实测修改时报错：write access denied
//         RequestUtils.writeProperty(localDevice,
//                 remoteDevice,
//                 objectIdentifier,
//                 PropertyIdentifier.outOfService,
//                 Boolean.TRUE); // com.serotonin.bacnet4j.type.primitive.Boolean
//         Thread.sleep(1000);*/
//         // 修改属性值
//         RequestUtils.writeProperty(localDevice,
//                 remoteDevice,
//                 objectIdentifier,
//                 propertyIdentifier,
//                 valueToWrite); // com.serotonin.bacnet4j.type.primitive.Double
// //        Thread.sleep(2000);
//         Encodable propertyValueAfterWrite = RequestUtils.getProperty(localDevice, remoteDevice, objectIdentifier, propertyIdentifier);
//         System.out.println("propertyValueAfterWrite  = " + propertyValueAfterWrite);
//
//         return propertyValueAfterWrite;
//     }
//
//     private static Map<PropertyIdentifier, Encodable> readPropertyValue(RemoteDevice remoteDevice, ObjectIdentifier objectIdentifier) throws BACnetException {
//         System.out.println("************************** 获取对象中属性的值 " + objectIdentifier.getInstanceNumber() + " ****************************");
//         PropertyIdentifier[] propertyIdentifiers = new PropertyIdentifier[]{
//                 PropertyIdentifier.objectName,
//                 PropertyIdentifier.description,
//                 // presentValue是该对象当前的值，其它属性都是描述这个对象的
//                 PropertyIdentifier.presentValue,
//                 PropertyIdentifier.eventState,
//                 PropertyIdentifier.outOfService,
//                 PropertyIdentifier.statusFlags
//         };
//         // 批量查询该对象的属性值
//         /*
//          * 三种方法的返回值对比：
//          * 方法三是返回: key: 属性id, value: 属性值；
//          * 方法二是返回: key: 对象id-属性id, value: 属性值；
//          * 方法一是返回: key: 设备id, value: (key: 对象id-属性id, value: 属性值)
//          */
//         // 方法一：PropertyUtils.readProperties，传入的是设备的id，而不是已经获取到的remoteDevice对象，返回的是 DeviceObjectPropertyValues
//         DeviceObjectPropertyReferences deviceObjectPropertyReferences = new DeviceObjectPropertyReferences();
//         deviceObjectPropertyReferences.add(remoteDevice.getInstanceNumber(), objectIdentifier, propertyIdentifiers);
//         DeviceObjectPropertyValues deviceObjectPropertyValues = PropertyUtils.readProperties(localDevice, deviceObjectPropertyReferences, null);
//
//         // 方法二：RequestUtils.readProperties，传入的是已经获取到的remoteDevice对象，可以指定是否允许为null，返回的是 PropertyValues
//         PropertyReferences propertyReferences = new PropertyReferences();
//         propertyReferences.add(objectIdentifier, propertyIdentifiers);
//         PropertyValues objectPropertyReferences = RequestUtils.readProperties(localDevice, remoteDevice, propertyReferences, true, null);
//         // 方法三：RequestUtils.getProperties， 传入的是已经获取到的remoteDevice对象，不可以指定是否允许为null，返回的是 Map<PropertyIdentifier, Encodable>
//         Map<PropertyIdentifier, Encodable> properties = RequestUtils.getProperties(localDevice, remoteDevice, objectIdentifier, null, propertyIdentifiers);
//
//         // 单独查询指定的属性值
//         // Encodable propertyEncodableValue = RequestUtils.getProperty(localDevice, remoteDevice, objectIdentifier, PropertyIdentifier.presentValue);
//
//         properties.forEach((propertyIdentifier, value) -> {
//             System.out.println(propertyIdentifier.toString() + ": " + value.toString());
//         });
//
//         return properties;
//     }
//
//     private static List<ObjectIdentifier> getObjectListOfDevice(RemoteDevice remoteDevice) throws BACnetException {
//         System.out.println("************************** 获取设备下的对象列表 " + remoteDevice.getInstanceNumber() + " ****************************");
//         List<ObjectIdentifier> objectList = RequestUtils.getObjectList(localDevice, remoteDevice).getValues();
//
//         // 列表中会将设备本身也返回回来，先排除掉吧，毕竟它不是设备下的对象
//         List<ObjectIdentifier> realObjectList = objectList.stream().filter(t -> !t.getObjectType().toString().equals("device")).collect(Collectors.toList());
//
//         realObjectList.forEach(object -> {
//             try {
//                 Encodable objectName = RequestUtils.readProperty(localDevice,
//                         remoteDevice,
//                         new ObjectIdentifier(object.getObjectType(), object.getInstanceNumber()),
//                         PropertyIdentifier.objectName,
//                         null);
//                 System.out.println("instanceNumber: " + object.getInstanceNumber() + ", objectType: " + object.getObjectType() + ", objectName: " + objectName);
//             } catch (BACnetException e) {
//                 e.printStackTrace();
//             }
//         });
//         return realObjectList;
//     }
//
//     private static RemoteDevice getSingleRemoteDevice(int remoteDeviceNumber) throws BACnetException {
//         System.out.println("************************** 获取指定的设备 " + remoteDeviceNumber + " ****************************");
//         // 还有另一种方法获取指定设备：localDevice.getRemoteDeviceBlocking(remoteDeviceNumber)
//         RemoteDevice remoteDeviceTemp = RemoteDeviceFinder.findDevice(localDevice, remoteDeviceNumber).get();
//         // 下一行的方法为jar包提供的工具方法，所做的内容就是通过 localDevice.send 获取属性值并赋值到remoteDevice上，以便直接通过remoteDevice.get...的方式调用
//         // Tips: 最好保留此方法，因为后续 RequestUtils.readProperty 等方法会使用到此工具方法中对 remoteDevice 这个对象上所赋的值（注释此行则可以看到NullPointerException的报错）
//         DiscoveryUtils.getExtendedDeviceInformation(localDevice, remoteDeviceTemp);
//
//         System.out.println("remoteDeviceTemp.getName() = " + remoteDeviceTemp.getName());
//         System.out.println("remoteDeviceTemp.getModelName() = " + remoteDeviceTemp.getModelName());
//         System.out.println("objectType: " + remoteDeviceTemp.getObjectIdentifier().getObjectType().toString() + ", instanceNumber: " + remoteDeviceTemp.getObjectIdentifier().getInstanceNumber());
//         return remoteDeviceTemp;
//     }
//
//     private static List<RemoteDevice> getAllRemoteDevices(String networkSegment) throws InterruptedException {
//         System.out.println("************************** 获取本网段所有的设备 " + networkSegment + " ****************************");
//         RemoteDeviceDiscoverer remoteDeviceDiscoverer = new RemoteDeviceDiscoverer(localDevice);
//         remoteDeviceDiscoverer.start();
//
//         // 等待网段内的设备响应广播消息
//         Thread.sleep(3000);
//
//         List<RemoteDevice> remoteDevices = remoteDeviceDiscoverer.getRemoteDevices();
//         System.out.println("remoteDevices.size() = " + remoteDevices.size());
//         remoteDevices.forEach(remoteDevice -> {
//             try {
//                 // 下一行的方法为jar包提供的工具方法，所做的内容就是通过 localDevice.send 获取属性值并赋值到remoteDevice上，以便直接通过remoteDevice.get...的方式调用
//                 DiscoveryUtils.getExtendedDeviceInformation(localDevice, remoteDevice);
//             } catch (BACnetException e) {
//                 e.printStackTrace();
//             }
//
//             System.out.println("device " + remoteDevice.getObjectIdentifier().getInstanceNumber() + ":");
//             System.out.println("    remoteDeviceTemp.getName() = " + remoteDevice.getName());
//             System.out.println("    remoteDeviceTemp.getModelName() = " + remoteDevice.getModelName());
//             System.out.println("    objectType: " + remoteDevice.getObjectIdentifier().getObjectType().toString() + ", instanceNumber: " + remoteDevice.getObjectIdentifier().getInstanceNumber());
//         });
//         remoteDeviceDiscoverer.stop();
//         return remoteDevices;
//     }
//
//
// }
//
