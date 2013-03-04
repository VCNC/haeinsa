package kr.co.vcnc.haeinsa.utils;

import java.util.Comparator;

/**
 * Wrapper of Comparator. It allow to compare element with null.
 * Null is assumed to be the smallest element.
 * @author Myungbo Kim
 *
 * @param <T> the type of element to compare with
 */
public class NullableComparator<T> implements Comparator<T> {
	private final Comparator<T> comparator;
	
	public NullableComparator(Comparator<T> comparator){
		this.comparator = comparator;
	}

	@Override
	public int compare(T o1, T o2) {
		if (o1 == null && o2 != null){
			return -1;
		}else if (o1 != null && o2 != null){
			return comparator.compare(o1, o2);
		}else if (o1 != null && o2 == null){
			return 1;
		}
		return 0;
	}
}
