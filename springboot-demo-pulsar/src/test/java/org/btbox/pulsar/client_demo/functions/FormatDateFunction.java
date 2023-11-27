package org.btbox.pulsar.client_demo.functions;

import org.apache.pulsar.functions.api.Context;
import org.apache.pulsar.functions.api.Function;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @description: pulsar Functions 日期转换
 * @author: BT-BOX
 * @createDate: 2023/11/27 9:41
 * @version: 1.0
 */
public class FormatDateFunction implements Function<String, String> {

    private SimpleDateFormat format1 = new SimpleDateFormat("yyyy/MM/dd HH/mm/ss");
    private SimpleDateFormat format2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");


    /**
     * 此方法，每接受到一条数据就会调用一次，process方式，其中
     * 参数1：输入的消息数据
     * 参数2：Context 表示上下文对象，用于执行一些相关的统计计算操作
     * @author: BT-BOX(HJH)
     * @param input
     * @param context
     * @version: 1.0
     * @createDate: 2023/11/27 9:45
     * @return: java.lang.String
     */
    @Override
    public String process(String input, Context context) throws Exception {
        Date oldDate = format1.parse(input);
        return format2.format(oldDate);
    }
}