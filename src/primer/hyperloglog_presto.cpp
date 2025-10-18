//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hyperloglog_presto.cpp
//
// Identification: src/primer/hyperloglog_presto.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "primer/hyperloglog_presto.h"

namespace bustub {

/** @brief Parameterized constructor. */
template <typename KeyType>
HyperLogLogPresto<KeyType>::HyperLogLogPresto(int16_t n_leading_bits) : cardinality_(0),leading_bits(n_leading_bits) {
	dense_bucket_.resize(1<<n_leading_bits,0);
}

/** @brief Element is added for HLL calculation. */
template <typename KeyType>
auto HyperLogLogPresto<KeyType>::AddElem(KeyType val) -> void {
	/** @TODO(student) Implement this function! */
	hash_t hash=CalculateHash(val);
	uint32_t idx=hash>>(sizeof(hash_t)*8-leading_bits);
	uint32_t bits=0;
	size_t mask=1;
	for(;bits<sizeof(hash_t)*8-leading_bits;bits++){
		if((mask<<bits)&hash){
			break;
		}
	}
	if(overflow_bucket_.count(idx)){
		if((overflow_bucket_[idx].to_ullong()<<DENSE_BUCKET_SIZE)+dense_bucket_[idx].to_ullong()>bits){
			return;
		}
	}
	else{
		if(dense_bucket_[idx].to_ullong()>bits){
			return;
		}
	}
	dense_bucket_[idx]=((1<<DENSE_BUCKET_SIZE)-1)&bits;
	if(bits>=(1<<DENSE_BUCKET_SIZE)){
		overflow_bucket_[idx]=bits>>DENSE_BUCKET_SIZE;
	}
}

/** @brief Function to compute cardinality. */
template <typename T>
auto HyperLogLogPresto<T>::ComputeCardinality() -> void {
	/** @TODO(student) Implement this function! */
	double_t sum=0;
	for(int i=0;i<1<<leading_bits;i++){
		uint32_t temp=0; 
		if(overflow_bucket_.count(i)){
			temp+=overflow_bucket_[i].to_ullong()<<DENSE_BUCKET_SIZE;
		}
		temp+=dense_bucket_[i].to_ullong();
		sum+=1.0/pow(2,temp);
	}
	cardinality_=CONSTANT*(1<<leading_bits)*(1<<leading_bits)/sum;
}

template class HyperLogLogPresto<int64_t>;
template class HyperLogLogPresto<std::string>;
}  // namespace bustub
