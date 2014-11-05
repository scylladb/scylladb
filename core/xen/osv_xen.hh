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
