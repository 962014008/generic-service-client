package top.itning.generic.service.core.service;

import com.alibaba.dubbo.rpc.RpcContext;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.netty.util.internal.StringUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.dubbo.common.utils.StringUtils;
import org.apache.dubbo.config.ApplicationConfig;
import org.apache.dubbo.config.ReferenceConfig;
import org.apache.dubbo.remoting.Constants;
import org.apache.dubbo.rpc.service.GenericService;
import org.slf4j.MDC;
import org.springframework.util.CollectionUtils;
import top.itning.generic.service.common.websocket.ProgressWebSocket;
import top.itning.generic.service.common.websocket.WebSocketMessageType;
import top.itning.generic.service.core.bo.DubboGenericRequestBO;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Dubbo泛化调用任务
 *
 * @author itning
 * @since 2021/1/3 15:22
 */
@SuppressWarnings("deprecation")
@Slf4j
public class DubboGenericInvokeTask implements Runnable {
    private static final Gson GSON_INSTANCE_WITH_PRETTY_PRINT = new GsonBuilder().setPrettyPrinting().create();

    private static final Gson GSON_INSTANCE = new Gson();

    private static final String MDC_TRADE_ID = "INNER_TRACE_ID";

    private final DubboGenericRequestBO dubboGenericRequestBO;
    private final ApplicationConfig applicationConfig;

    public DubboGenericInvokeTask(ApplicationConfig applicationConfig, DubboGenericRequestBO dubboGenericRequestBO1) {
        this.applicationConfig = applicationConfig;
        this.dubboGenericRequestBO = dubboGenericRequestBO1;
    }

    @Override
    public void run() {
        ReferenceConfig<GenericService> reference = null;
        try {
            putTrace();
            reference = new ReferenceConfig<>();
            // 弱类型接口名
            reference.setInterface(dubboGenericRequestBO.getInterfaceName());
            // 声明为泛化接口
            reference.setGeneric(true);

            reference.setApplication(applicationConfig);

            reference.setGroup(dubboGenericRequestBO.getGroup());

            reference.setVersion(dubboGenericRequestBO.getVersion());

            reference.setCheck(false);

            reference.setUrl(dubboGenericRequestBO.getUrl());

            reference.setRetries(0);

            reference.setTimeout(5000);

            reference.setParameters(Collections.singletonMap(Constants.RECONNECT_KEY, "false"));

            GenericService genericService = reference.get();

            List<String> parameterTypeList = new ArrayList<>();
            List<Object> argList = new ArrayList<>();
            if (!CollectionUtils.isEmpty(dubboGenericRequestBO.getParams())) {
                dubboGenericRequestBO.getParams().forEach(item -> {
                    // 完整的类型，可能包含具体泛型的具体类型
                    String fullType = item.keySet().toArray(new String[]{})[0];

                    // 将外部类型和内部类型（即泛化调用-泛型的具体类型，如BaseReqDto<Xxx>的Xxx的具体类型=com.Xxx）解析出来
                    // 1、外部类型
                    String outerType = fullType;
                    // 2、内部类型
                    String innerType = StringUtil.EMPTY_STRING;
                    if (fullType.contains("<") && fullType.contains(">")) {
                        int indexOfBegin = fullType.indexOf("<");
                        int indexOfEnd = fullType.indexOf(">");
                        outerType = fullType.substring(0, indexOfBegin);
                        innerType = fullType.substring(indexOfBegin + 1, indexOfEnd);
                    }

                    // 设置外部类型
                    parameterTypeList.add(outerType);

                    // 设置内部类型
                    Map outerMap = (Map) item.get(fullType);
                    Map innerMap = (Map) outerMap.get("param");
                    if (StringUtils.isNotEmpty(innerType)) {
                        innerMap.put("class", innerType);
                    }

                    argList.add(outerMap);
                });
            }

            Object result = genericService.$invoke(dubboGenericRequestBO.getMethod(), parameterTypeList.toArray(new String[]{}), argList.toArray());

            if (null == result) {
                log.info("Result Is Null");
                ProgressWebSocket.sendMessage(dubboGenericRequestBO.getToken(), dubboGenericRequestBO.getEcho(), "调用成功：入参[" + GSON_INSTANCE.toJson(dubboGenericRequestBO) + "]\n");
                return;
            }

            log.info("Result Type：{} Value:{}", result.getClass().getName(), result);

            String jsonString = GSON_INSTANCE_WITH_PRETTY_PRINT.toJson(result);

            ProgressWebSocket.sendMessage(dubboGenericRequestBO.getToken(), dubboGenericRequestBO.getEcho(), "调用成功：TraceID[" + MDC.get(MDC_TRADE_ID) + "] 入参[" + GSON_INSTANCE.toJson(dubboGenericRequestBO) + "]\n>>>\n");
            ProgressWebSocket.sendMessage(dubboGenericRequestBO.getToken(), dubboGenericRequestBO.getEcho(), WebSocketMessageType.JSON, jsonString);
        } catch (Throwable e) {
            log.warn("Invoke Error：", e);
            ProgressWebSocket.sendMessage(dubboGenericRequestBO.getToken(), dubboGenericRequestBO.getEcho(), "调用失败：入参[" + GSON_INSTANCE.toJson(dubboGenericRequestBO) + "]\n Msg：" + e.getMessage());
        } finally {
            if (null != reference) {
                reference.destroy();
            }
            ProgressWebSocket.sendMessage(dubboGenericRequestBO.getToken(), dubboGenericRequestBO.getEcho(), "执行完成");
            MDC.remove(MDC_TRADE_ID);
        }
    }

    private void putTrace() {
        long currentTime = System.currentTimeMillis();
        long timeStamp = currentTime % 1000000L;
        int randomNumber = ThreadLocalRandom.current().nextInt(1000000);
        String traceId = "TB" + Long.toHexString(timeStamp * 10000 + randomNumber).toUpperCase();
        RpcContext.getContext().setAttachment(MDC_TRADE_ID, traceId);
        MDC.put(MDC_TRADE_ID, traceId);
    }
}