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
#ifndef _XEN_HH
#define _XEN_HH
// Those should come directly from the OSv three. However, the xen exported
// functions are not currently living in osv/include, but spread around the
// BSD directory. We should move them there ASAP and then use them here.
extern int
bind_listening_port_to_irq(unsigned int remote_domain, int * port);
extern int
evtchn_from_irq(int irq);
extern int
notify_remote_via_evtchn(int port);
extern void
unmask_evtchn(int port);
extern "C" int
intr_add_handler(const char *name, int vector, void *filter,
                 void (*handler)(void *arg), void *arg, int flags,
                 void **cookiep);

extern int
gnttab_alloc_grant_references(uint16_t count, uint32_t *head);
extern int
gnttab_claim_grant_reference(uint32_t *private_head);

extern int
gnttab_grant_foreign_access(uint16_t domid, unsigned long frame, int readonly, uint32_t *result);
extern void gnttab_grant_foreign_access_ref(uint32_t ref, uint16_t domid, unsigned long frame, int readonly);
extern void
gnttab_release_grant_reference(uint32_t *private_head, unsigned ref);
extern int
gnttab_end_foreign_access_ref(unsigned ref);

extern "C" uint64_t
virt_to_phys(void *virt);
#endif
