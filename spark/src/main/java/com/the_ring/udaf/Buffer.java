package com.the_ring.udaf;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class Buffer implements Serializable {
    private Long sum;
    private Long count;
}
