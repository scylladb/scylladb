/*
 * This file is open source software, licensed to you under the terms
 * of the Apache License, Version 2.0 (the "License").  See the NOTICE file
 * distributed with this work for additional information regarding copyright
 * ownership.  You may not use this file except in compliance with the License.
 *
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
/*
 * Copyright (C) 2014 Cloudius Systems, Ltd.
 *
 */

#ifndef VIRTIO_INTERFACE_HH_
#define VIRTIO_INTERFACE_HH_

#ifndef _LINUX_VIRTIO_RING_H
#define _LINUX_VIRTIO_RING_H

/* An interface for efficient virtio implementation, currently for use by KVM
 * and lguest, but hopefully others soon.  Do NOT change this since it will
 * break existing servers and clients.
 *
 * This header is BSD licensed so anyone can use the definitions to implement
 * compatible drivers/servers.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 * 3. Neither the name of IBM nor the names of its contributors
 *    may be used to endorse or promote products derived from this software
 *    without specific prior written permission.
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS ``AS IS'' AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED.  IN NO EVENT SHALL IBM OR CONTRIBUTORS BE LIABLE
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS
 * OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
 * HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
 * LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY
 * OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
 * SUCH DAMAGE.
 *
 * Copyright Rusty Russell IBM Corporation 2007. */

/* This marks a buffer as continuing via the next field. */
#define VRING_DESC_F_NEXT       1
/* This marks a buffer as write-only (otherwise read-only). */
#define VRING_DESC_F_WRITE      2
/* This means the buffer contains a list of buffer descriptors. */
#define VRING_DESC_F_INDIRECT   4

/* The Host uses this in used->flags to advise the Guest: don't kick me when
 * you add a buffer.  It's unreliable, so it's simply an optimization.  Guest
 * will still kick if it's out of buffers. */
#define VRING_USED_F_NO_NOTIFY  1
/* The Guest uses this in avail->flags to advise the Host: don't interrupt me
 * when you consume a buffer.  It's unreliable, so it's simply an
 * optimization.  */
#define VRING_AVAIL_F_NO_INTERRUPT      1

/* We support indirect buffer descriptors */
#define VIRTIO_RING_F_INDIRECT_DESC     (1 << 28)

/* The Guest publishes the used index for which it expects an interrupt
 * at the end of the avail ring. Host should ignore the avail->flags field. */
/* The Host publishes the avail index for which it expects a kick
 * at the end of the used ring. Guest should ignore the used->flags field. */
#define VIRTIO_RING_F_EVENT_IDX         (1 << 29)

/* The standard layout for the ring is a continuous chunk of memory which looks
 * like this.  We assume num is a power of 2.
 *
 * struct vring
 * {
 *      // The actual descriptors (16 bytes each)
 *      struct vring_desc desc[num];
 *
 *      // A ring of available descriptor heads with free-running index.
 *      uint16_t avail_flags;
 *      uint16_t avail_idx;
 *      uint16_t available[num];
 *      uint16_t used_event_idx;
 *
 *      // Padding to the next align boundary.
 *      char pad[];
 *
 *      // A ring of used descriptor heads with free-running index.
 *      uint16_t used_flags;
 *      uint16_t used_idx;
 *      struct vring_used_elem used[num];
 *      uint16_t avail_event_idx;
 * };
 */

#endif

#define VIRTIO_NET_F_CSUM (1 << 0)
#define VIRTIO_NET_F_GUEST_CSUM (1 << 1)
#define VIRTIO_NET_F_CTRL_GUEST_OFFLOADS (1 << 2)
#define VIRTIO_NET_F_MAC (1 << 5)
#define VIRTIO_NET_F_GUEST_TSO4 (1 << 7)
#define VIRTIO_NET_F_GUEST_TSO6 (1 << 8)
#define VIRTIO_NET_F_GUEST_ECN (1 << 9)
#define VIRTIO_NET_F_GUEST_UFO (1 << 10)
#define VIRTIO_NET_F_HOST_TSO4 (1 << 11)
#define VIRTIO_NET_F_HOST_TSO6 (1 << 12)
#define VIRTIO_NET_F_HOST_ECN (1 << 13)
#define VIRTIO_NET_F_HOST_UFO (1 << 14)
#define VIRTIO_NET_F_MRG_RXBUF (1 << 15)
#define VIRTIO_NET_F_STATUS (1 << 16)
#define VIRTIO_NET_F_CTRL_VQ (1 << 17)
#define VIRTIO_NET_F_CTRL_RX (1 << 18)
#define VIRTIO_NET_F_CTRL_VLAN (1 << 19)
#define VIRTIO_NET_F_GUEST_ANNOUNCE (1 << 21)
#define VIRTIO_NET_F_MQ (1 << 22)
#define VIRTIO_NET_F_CTRL_MAC_ADDR (1 << 23)

#endif /* VIRTIO_INTERFACE_HH_ */
