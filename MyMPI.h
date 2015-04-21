/*
 * MyMPI.h
 * Copyright (C) 2015 jiangxin <jiangxinnju@163.com>
 *
 * Distributed under terms of the MIT license.
 */

#ifndef MYMPI_H
#define MYMPI_H

#define MAX(a, b) ((a) > (b) ? (a) : (b))
#define MIN(a, b) ((a) < (b) ? (a) : (b))
#define BLOCK_LOW(id, p, n) ((id)*(n)/(p))
#define BLOCK_HIGH(id, p, n) (BLOCK_LOW((id)+1,p,n)-1)
#define BLOCK_SIZE(id, p, n) (BLOCK_HIGH(id,p,n)-BLOCK_LOW(id,p,n)+1)
#define BLOCK_OWMER(j,p,n) (((p)*((j)+1)-1)/(n))

#endif /* !MYMPI_H */
