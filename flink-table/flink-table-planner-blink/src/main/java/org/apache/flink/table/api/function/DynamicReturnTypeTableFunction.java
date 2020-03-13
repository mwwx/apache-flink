package org.apache.flink.table.api.function;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;

/**
 * DynamicReturnTypeTableFunction.
 */
public abstract class DynamicReturnTypeTableFunction extends TableFunction<Row> {

	private static long funcIndex = 0;

	public DynamicReturnTypeTableFunction() {
		Class<? extends DynamicReturnTypeTableFunction> currentClass = getClass();

		//check subClass must have a empty constructor
		try {
			currentClass.getConstructor();
		} catch (NoSuchMethodException e) {
			throw new RuntimeException(String.format("Class %s must have a public and empty constructor.", getClass().getName()));
		}
		//register function
		TableFunctionInterceptor.getInstance().register(getFunctionName(), this);
	}

	protected abstract RowTypeInfo getReturnType();

	public abstract String getFunctionName();

	public DynamicReturnTypeTableFunction newInstance(Object[] args) {
		Constructor<?>[] constructors = this.getClass().getConstructors();
		//参数个数和类型都得匹配
		try {
			for (Constructor<?> constructorI : constructors) {
				Constructor<DynamicReturnTypeTableFunction> constructor = (Constructor<DynamicReturnTypeTableFunction>) constructorI;
				int parameterCount = constructor.getParameterCount();
				if (parameterCount == args.length) {
					Class[] parameterTypes = constructor.getParameterTypes();
					boolean match = true;
					for (int i = 0; i < parameterTypes.length; i++) {
						Class<?> type = parameterTypes[i];
						if (args[i] == null) {
							if (type.isPrimitive()) {
								throw new RuntimeException(String.format("the param %d was primitive type can't assign from null value.", i + 1));
							}
							args[i] = null;
						} else {
							match = match & type.isAssignableFrom(args[i].getClass());
						}
					}
					if (match) {
						return constructor.newInstance(args);
					}
				}
			}
		} catch (InstantiationException | IllegalAccessException | InvocationTargetException e) {
			throw new RuntimeException(e);
		}
		throw new RuntimeException("can't find suitable constructor for args: " + Arrays.toString(args));
	}

	@Override
	public TypeInformation<Row> getResultType() {
		RowTypeInfo typeInfo = getReturnType();
		if (typeInfo != null) {
			return typeInfo;
		}
		return new RowTypeInfo();
	}

	public String getNextInternalCatalogFunctionName() {
		return getFunctionName() + "_" + funcIndex++;
	}

}
