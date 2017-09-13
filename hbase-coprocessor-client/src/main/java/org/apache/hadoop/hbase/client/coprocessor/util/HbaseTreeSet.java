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

import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.TreeSet;

public class HbaseTreeSet<E> extends TreeSet<E> {
	
	private static final long serialVersionUID = 6494042821408729671L;

	@Override
	public boolean add(E e) {
		return super.add((E)format(e));
	}

	@Override
	public boolean addAll(Collection<? extends E> c) {
		return super.addAll(replaceColl(c));
	}
	
	@SuppressWarnings("unchecked")
	private E format(E e){
		if (e == null) {
			return e;
		}
		if (e instanceof String) {
			return(E) HbaseStringUtil.formatString((String)e);
		}
		return e;
	}
	
	@SuppressWarnings("unchecked")
	private Collection<? extends E> replaceColl(Collection<? extends E> c){
		if (c == null) {
			return null;
		}
		Collection<String> cc=null;
		if(c.iterator().next() instanceof String){
			try {
				cc=(Collection<String>) Class.forName(c.getClass().getName()).newInstance();
				Iterator<? extends E> it = c.iterator();
				while (it.hasNext()) {
					E e = it.next();
					E a = format(e);
					cc.add((String)a);
				}
				return (Collection<? extends E>) cc;
			} catch (InstantiationException | IllegalAccessException
					| ClassNotFoundException e) {
				e.printStackTrace();
			} 
		}
		return c;
	} 
	
	public static void main(String[] args) {
		HbaseTreeSet<String> hh=new HbaseTreeSet<String>();
//		hh.add("asdas");
//		hh.add("asdas_2");
//		hh.add("asdas_qwe::123");
//		hh.add("asdas_qwe|123");
//		hh.add("asdas_qwe#123##123");
		LinkedHashSet<String> ll=new LinkedHashSet<String>();
		ll.add("asdas");
		ll.add("asdas_2");
		ll.add("asdas_2");
		ll.add("asdas_qwe::123");
		ll.add("asdas_qwe|123");
		ll.add("asdas_qwe#123##123");
		hh.addAll(ll);
		System.out.println();
	}
}
