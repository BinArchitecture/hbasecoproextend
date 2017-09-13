/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.client.coprocessor.util;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;


/**
 *
 */
public class ReFelctionUtil
{
	@SuppressWarnings("unchecked")
	public static <T> T getDynamicObj(Class<?> clazz,String name,Object instance) throws Exception  {
		try {
			Field ff = clazz.getDeclaredField(name);
			ff.setAccessible(true);
			return (T)ff.get(instance);
		} catch (NoSuchFieldException | SecurityException | IllegalArgumentException | IllegalAccessException e) {
			throw e;
		}
	}
	
	public static void setFinalValue(Object obj,Field field, Object newValue) throws Exception {
	      try {
			field.setAccessible(true);
			  Field modifiersField = Field.class.getDeclaredField("modifiers");
			  modifiersField.setAccessible(true);
			  modifiersField.setInt(field, field.getModifiers() & ~Modifier.FINAL);
			  field.set(obj, newValue);
			  modifiersField.setAccessible(false);  
			  field.setAccessible(false);
		} catch (SecurityException | NoSuchFieldException
				| IllegalArgumentException | IllegalAccessException e) {
			throw e;
		}  
	   }
	
	
	public static Object dynamicInvokePriMethods(Class<?> clazz,final String tbName, final Class<?>[] types,Object instance, final Object... args) throws Exception
	{
		try {
			Method method = clazz.getDeclaredMethod(tbName, types);
			method.setAccessible(true);
			return method.invoke(instance, args);
		} catch (NoSuchMethodException | SecurityException
				| IllegalAccessException | IllegalArgumentException
				| InvocationTargetException e) {
			throw e;
		}
	}
}
