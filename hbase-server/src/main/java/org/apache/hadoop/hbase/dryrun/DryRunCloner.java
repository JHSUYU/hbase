package org.apache.hadoop.hbase.dryrun;

import org.apache.yetus.audience.InterfaceAudience;
import java.lang.reflect.Field;
import java.lang.reflect.Constructor;

@InterfaceAudience.Private
public class DryRunCloner {
    @SuppressWarnings("unchecked")
    public static <T> T shallowClone(T originalObject) {
        if (originalObject == null) {
            return null;
        }

        Class<T> clazz = (Class<T>) originalObject.getClass();
        T newObject = createInstance(clazz);

        if (newObject == null) {
            return null;
        }

        try {
            Field[] fields = clazz.getDeclaredFields();
            for (Field field : fields) {
                field.setAccessible(true);
                Object value = field.get(originalObject);
                field.set(newObject, value);
            }
        } catch (Exception e) {
            return null;
        }

        return newObject;
    }

    private static <T> T createInstance(Class<T> clazz) {
        try {
            return clazz.getDeclaredConstructor().newInstance();
        } catch (Exception e) {
            for (Constructor<?> constructor : clazz.getDeclaredConstructors()) {
                try {
                    constructor.setAccessible(true);
                    Class<?>[] paramTypes = constructor.getParameterTypes();
                    Object[] args = new Object[paramTypes.length];
                    for (int i = 0; i < paramTypes.length; i++) {
                        args[i] = getDefaultValue(paramTypes[i]);
                    }
                    return clazz.cast(constructor.newInstance(args));
                } catch (Exception ignored) {
                }
            }
        }
        return null;
    }

    private static Object getDefaultValue(Class<?> clazz) {
        if (clazz.isPrimitive()) {
            if (clazz == boolean.class) return false;
            if (clazz == char.class) return '\u0000';
            if (clazz == byte.class) return (byte) 0;
            if (clazz == short.class) return (short) 0;
            if (clazz == int.class) return 0;
            if (clazz == long.class) return 0L;
            if (clazz == float.class) return 0.0f;
            if (clazz == double.class) return 0.0d;
        }
        return null;
    }
}
