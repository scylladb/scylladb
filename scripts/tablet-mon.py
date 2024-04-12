#!/bin/env python
#
# Copyright (C) 2023-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This is a tool for live-monitoring the state of tablets and load balancing dynamics in a Scylla cluster.
#
# WARNING: This is not an officially supported tool. It is subject to arbitrary changes without notice.
#
# Usage:
#
#   ./tablet-mon.py
#
# Tries to connect to a CQL server on localhost
#
# To connect to a different host, you can set up a tunnel:
#
#   ssh -L *:9042:127.0.0.1:9042 -N <remote-host>
#
# Key bindings:
#
#  t - toggle display of table tags. Each table has a unique color which is displayed in the bottom part of the tablet.
#

import math
import threading
import time
import logging
import pygame

from cassandra.cluster import Cluster

# Layout settings
tablet_size = 60
node_frame_size = 18
node_frame_thickness = 3
frame_size = 30
shard_spacing = 0
node_frame_mid = node_frame_size // 2

tablet_w = tablet_size
tablet_h = tablet_w // 2
tablet_radius = tablet_size // 7
tablet_frame_size = max(2, min(6, tablet_size // 14))
show_table_tag = False

# Animation settings
streaming_trace_duration_ms = 300
streaming_trace_decay_ms = 2000
streaming_done_trace_duration_ms = 40
streaming_done_trace_decay_ms = 1000
insert_time_ms = 500
fall_acceleration = 3000
fall_delay_ms = 0

# Displayed objects
nodes = []
nodes_by_id = {}
tracers = list()
trace_lines = set()
trace_lines_pool = list()

cql_update_timeout_ms = 800
cql_update_period = 0.2


class Node(object):
    def __init__(self, id):
        self.shards = []
        self.id = id


class Shard(object):
    def __init__(self):
        self.tablets = {}

    def ordered_tablets(self):
        return sorted(self.tablets.values(), key=lambda t: t.id)


class Tablet(object):
    STATE_NORMAL = 0
    STATE_JOINING = 1
    STATE_LEAVING = 2

    def __init__(self, id, state, initial):
        self.id = id
        self.state = state
        self.insert_time = pygame.time.get_ticks()
        self.streaming = False

        table_id = id[0]
        if table_id not in table_tag_colors:
            table_tag_colors[table_id] = table_tag_palette[len(table_tag_colors) % len(table_tag_palette)]
        self.table_tag_color = table_tag_colors[table_id]

        # Updated by animate()
        self.h = 0 # Height including tablet_frame_size
        self.pos = 0 # Position from the bottom of shard column
        self.size_frac = 0
        self.speed = 0
        self.fall_start = None

        if initial:
            self.h = tablet_h + tablet_frame_size * 2
            self.size_frac = 1

        # Updated by redraw()
        self.x = 0
        self.y = 0

    def get_mid_x(self):
        return self.x + tablet_frame_size + float(tablet_w) / 2

    def get_mid_y(self):
        return self.y + float(self.h) / 2


def cubic_ease_out(t):
    # if t < 0.5:
    #     return 4 * t * t * t
    # else:
    #     t -= 1
    #     return 1 + 4 * t * t * t

    return t * t * t

def cubic_ease_in(t):
    t = 1.0 - t
    return 1.0 - t * t * t


class TraceLine(object):
    """
    Fading line which is left behind by a Tracer
    """

    def __init__(self, x, y, x2, y2, color, thickness, decay_ms, now):
        self.x = x
        self.y = y
        self.x2 = x2
        self.y2 = y2
        self.color = color
        self.thickness = thickness
        self.start_time = now
        self.decay_ms = decay_ms

        # Surface position
        self.sx = self.x - thickness/2
        self.sy = self.y - thickness/2

        # Convert to surface coordinates
        self.x -= self.sx
        self.y -= self.sy
        self.x2 -= self.sx
        self.y2 -= self.sy

        # Translate to ensure positive coordinates within surface
        if self.x2 < 0:
            self.sx += self.x2
            self.x -= self.x2
            self.x2 = 0

        if self.y2 < 0:
            self.sy += self.y2
            self.y -= self.y2
            self.y2 = 0

        size = (abs(int(self.x2 - self.x)) + thickness, abs(int(self.y2 - self.y)) + thickness)
        self.surf = pygame.Surface(size)
        self.surf.set_colorkey((0, 0, 0))

    def draw(self, now):
        time_left = self.decay_ms - (now - self.start_time)
        if time_left <= 0:
            return False
        frac = cubic_ease_out(time_left / self.decay_ms)
        alpha = int(250 * frac)
        color = (self.color[0], self.color[1], self.color[2])

        self.surf.set_alpha(alpha)
        self.surf.fill((0, 0, 0))
        # pygame.draw.circle(surf, color, (int(self.x2 + self.x)/2, int(self.y2 + self.y)/2), int(self.thickness/2 * frac))
        pygame.draw.line(self.surf, color, (int(self.x), int(self.y)), (int(self.x2), int(self.y2)),
                         int(self.thickness * frac))
        window.blit(self.surf, (self.sx, self.sy))
        return True


def add_trace_line(x, y, x2, y2, color, thickness, decay_ms, now):
    if trace_lines_pool:
        t = trace_lines_pool.pop()
        t.__init__(x, y, x2, y2, color, thickness, decay_ms, now)
        trace_lines.add(t)
    else:
        trace_lines.add(TraceLine(x, y, x2, y2, color, thickness, decay_ms, now))


def draw_trace_lines(trace_lines, now):
    global changed
    for tl in list(trace_lines):
        changed = True
        if not tl.draw(now):
            trace_lines_pool.append(tl)
            trace_lines.remove(tl)


class HourGlass(object):
    def __init__(self, now):
        self.last_now = now
        self.period_ms = 500
        self.decay_ms = 300
        self.trace_lines = []
        self.x = None
        self.y = None

    def draw(self, now):
        cx = window.get_size()[0] / 2
        cy = window.get_size()[1] / 2
        size = window.get_size()[0] / 5

        new_x = math.sin((now % self.period_ms) / float(self.period_ms) * 2 * math.pi) * size / 2 + cx
        new_y = math.cos((now % self.period_ms) / float(self.period_ms) * 2 * math.pi) * size / 2 + cy

        if self.x:
            self.trace_lines.append(TraceLine(self.x, self.y, new_x, new_y, (130, 130, 255), 30, self.decay_ms, now))

        self.x = new_x
        self.y = new_y

        draw_trace_lines(self.trace_lines, now)


class Tracer(object):
    """
    Shooting star which goes from one tablet to another
    and leaves a fading trace line behind it
    """

    def get_src_tablet(self):
        return nodes_by_id[self.src_replica[0]].shards[self.src_replica[1]].tablets[self.tablet_id]

    def get_dst_tablet(self):
        return nodes_by_id[self.dst_replica[0]].shards[self.dst_replica[1]].tablets[self.tablet_id]

    def __init__(self, tablet_id, src_replica, dst_replica, src_color, dst_color, thickness, decay_ms, duration_ms):
        self.tablet_id = tablet_id
        self.src_replica = src_replica
        self.dst_replica = dst_replica
        # Position can be calculated after first redraw() which lays out new tablets
        self.x = None
        self.y = None
        self.end_time = None
        self.last_now = None

        self.decay_ms = decay_ms
        self.duration_ms = duration_ms
        self.src_color = src_color
        self.dst_color = dst_color
        self.thickness = thickness

    def move(self, now):
        def interpolate_color(frac, c1, c2):
            return pygame.Color(
                int(c1[0] + (c2[0] - c1[0]) * frac),
                int(c1[1] + (c2[1] - c1[1]) * frac),
                int(c1[2] + (c2[2] - c1[2]) * frac)
            )

        try:
            dst_x = self.get_dst_tablet().get_mid_x()
            dst_y = self.get_dst_tablet().get_mid_y()

            if not self.last_now:
                self.end_time = now + self.duration_ms

                self.x = self.get_src_tablet().get_mid_x()
                self.y = self.get_src_tablet().get_mid_y()

                time_left = self.end_time - now
                self.vx = (dst_x - self.x) / time_left
                self.vy = (dst_y - self.y) / time_left
                self.vy -= abs(self.vx) / 2 # Give higher vertical speed for arced trajectory
                self.last_now = now
                return True

            if now == self.last_now:
                return True

            g_dt = float(now - self.last_now)
            if now >= self.end_time:
                g_dt = self.end_time - self.last_now
            g_last_now = self.last_now

            # Subdivide for smoother lines at high speeds
            segment_length = 30
            distance = math.sqrt((self.vx * g_dt) ** 2 + (self.vy * g_dt) ** 2)
            n_segments = max(1, int(distance / segment_length))

            for i in range(n_segments):
                l_now = g_last_now + g_dt * (i + 1) / n_segments
                dt = float(l_now - self.last_now)

                time_left = self.end_time - self.last_now
                vx = (dst_x - self.x) / time_left
                vy = (dst_y - self.y) / time_left

                steer_force = dt / time_left
                self.vx = steer_force * vx + (1.0 - steer_force) * self.vx
                self.vy = steer_force * vy + (1.0 - steer_force) * self.vy

                frac = 1.0 - (self.end_time - self.last_now - dt) / self.duration_ms
                new_x = self.x + self.vx * dt
                new_y = self.y + self.vy * dt

                add_trace_line(self.x, self.y, new_x, new_y,
                               interpolate_color(frac, self.src_color, self.dst_color), self.thickness, self.decay_ms, l_now)

                self.x = new_x
                self.y = new_y
                self.last_now = l_now

            return now < self.end_time
        except KeyError: # src or dst disappeared
            return False


def fire_tracer(tablet_id, src_replica, dst_replica, src_color, dst_color, thickness, decay_ms, duration_ms):
    tracers.append(Tracer(tablet_id, src_replica, dst_replica, src_color, dst_color, thickness, decay_ms, duration_ms))


def move_tracers(now):
    global changed
    for tracer in list(tracers):
        changed = True
        if not tracer.move(now):
            tracers.remove(tracer)


BLACK = (0, 0, 0)
WHITE = (255, 255, 255)
GRAY = (192, 192, 192)
light_yellow = (250, 240, 200)
even_darker_purple = (190, 180, 190)
dark_purple = (205, 195, 205)
light_purple = (240, 200, 255)
dark_red = (215, 195, 195)
dark_green = (195, 215, 195)
light_red = (255, 200, 200)
light_green = (200, 255, 200)
light_gray = (240, 240, 240)

tablet_colors = {
    (Tablet.STATE_NORMAL, None): GRAY,
    (Tablet.STATE_JOINING, 'allow_write_both_read_old'): dark_green,
    (Tablet.STATE_LEAVING, 'allow_write_both_read_old'): dark_red,
    (Tablet.STATE_JOINING, 'write_both_read_old'): dark_green,
    (Tablet.STATE_LEAVING, 'write_both_read_old'): dark_red,
    (Tablet.STATE_JOINING, 'streaming'): light_green,
    (Tablet.STATE_LEAVING, 'streaming'): light_red,
    (Tablet.STATE_JOINING, 'write_both_read_new'): light_yellow,
    (Tablet.STATE_LEAVING, 'write_both_read_new'): even_darker_purple,
    (Tablet.STATE_JOINING, 'use_new'): light_yellow,
    (Tablet.STATE_LEAVING, 'use_new'): dark_purple,
    (Tablet.STATE_JOINING, 'cleanup'): light_yellow,
    (Tablet.STATE_LEAVING, 'cleanup'): light_purple,
    (Tablet.STATE_JOINING, 'end_migration'): light_yellow,
    (Tablet.STATE_LEAVING, 'end_migration'): light_gray,
}

table_tag_palette = [
    (238, 187, 187),  # Light Red
    (238, 221, 187),  # Orange
    (187, 238, 187),  # Lime Green
    (187, 238, 238),  # Aqua
    (187, 221, 238),  # Light Blue
    (221, 187, 238),  # Lavender
    (238, 187, 238),  # Pink
    (238, 238, 187),  # Yellow
    (238, 153, 153),  # Lighter Red
    (238, 187, 153),  # Lighter Orange
    (238, 238, 153),  # Lighter Yellow
    (153, 238, 153),  # Lighter Green
    (153, 221, 238),  # Lighter Blue
    (187, 153, 238),  # Lighter Purple
    (238, 153, 238),  # Lighter Pink
    (204, 204, 204)   # Gray
]

# Map between table id and color
table_tag_colors = {}

# Avoid redrawing if nothing changed
changed = False

cluster = Cluster(['127.0.0.1'])
session = cluster.connect()
topo_query = session.prepare("SELECT host_id, shard_count FROM system.topology")
tablets_query = session.prepare("SELECT * FROM system.tablets")

data_source_alive = False

def update_from_cql(initial=False):
    """
    Updates objects using CQL queries of system state

    :param initial: When True, disables animations which indicate changes.
                    We don't want initial state to appear as if everything suddenly changed.
    """

    global changed
    global data_source_alive

    all_host_ids = set()
    for host in session.execute(topo_query):
        id = host.host_id
        all_host_ids.add(id)
        if id not in nodes_by_id:
            n = Node(id)
            nodes.append(n)
            nodes_by_id[id] = n
            changed = True
        n = nodes_by_id[id]
        if len(n.shards) > host.shard_count:
            n.shards = n.shards[:host.shard_count]
        while len(n.shards) < host.shard_count:
            n.shards.append(Shard())


    for id in nodes_by_id:
        if id not in all_host_ids:
            nodes.remove(nodes_by_id[id])
            del nodes_by_id[id]
            changed = True

    tablets_by_shard = set()
    for tablet in session.execute(tablets_query):
        id = (tablet.table_id, tablet.last_token)
        replicas = set(tablet.replicas)
        new_replicas = set(tablet.new_replicas) if tablet.new_replicas else replicas

        leaving = replicas - new_replicas
        joining = new_replicas - replicas

        stage_change = False
        inserted = False
        for replica in replicas.union(new_replicas):
            host = replica[0]
            shard = replica[1]

            if host not in nodes_by_id:
                continue

            tablets_by_shard.add((host, shard, id))

            if replica in joining:
                state = (Tablet.STATE_JOINING, tablet.stage)
            elif replica in leaving:
                state = (Tablet.STATE_LEAVING, tablet.stage)
            else:
                state = (Tablet.STATE_NORMAL, None)

            s = nodes_by_id[host].shards[shard]
            if id not in s.tablets:
                s.tablets[id] = Tablet(id, state, initial=initial)
                stage_change = True
                inserted = True
                changed = True
            t = s.tablets[id]
            if t.state != state:
                t.state = state
                stage_change = True
                changed = True
                if not initial and t.streaming:
                    if tablet.stage != "streaming" and tablet.stage != "write_both_read_old":
                        dst = t.streaming
                        t.streaming = None
                        fire_tracer(id, replica, dst, light_purple, light_yellow, tablet_h * 0.7,
                                    streaming_done_trace_decay_ms, streaming_done_trace_duration_ms)

        if not initial and stage_change and len(leaving) == 1 and len(joining) == 1:
            src = leaving.pop()
            dst = joining.pop()
            src_tablet = nodes_by_id[src[0]].shards[src[1]].tablets[id]
            if inserted:
                src_tablet.streaming = dst
                fire_tracer(id, src, dst, light_red, light_green, tablet_h,
                            streaming_trace_decay_ms, streaming_trace_duration_ms)

    for n in nodes:
        for s_idx, s in enumerate(n.shards):
            for t in list(s.tablets.keys()):
                if (n.id, s_idx, t) not in tablets_by_shard:
                    del s.tablets[t]
                    changed = True


def cql_updater():
    global data_source_alive
    while True:
        try:
            update_from_cql()
            data_source_alive = True
        except Exception as e:
            print(e)
        time.sleep(cql_update_period)


# Set the logging level to DEBUG for the Cassandra driver
cassandra_logger = logging.getLogger('cassandra')
cassandra_logger.setLevel(logging.DEBUG)

# Optionally, configure the logger to print DEBUG messages to the console
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.DEBUG)
cassandra_logger.addHandler(console_handler)

update_from_cql(initial=True)

cassandra_thread = threading.Thread(target=cql_updater)
cassandra_thread.daemon = True
cassandra_thread.start()

pygame.init()
clock = pygame.time.Clock()

# Compute initial layout
max_tablet_count = 0
window_width = frame_size * 2
for node in nodes:
    window_width += node_frame_size * 2
    for shard in node.shards:
        max_tablet_count = max(max_tablet_count, len(shard.tablets))
        window_width += shard_spacing + tablet_w + tablet_frame_size * 2

window_height = frame_size * 2 + (tablet_h + tablet_frame_size * 2) * max_tablet_count + (node_frame_size * 2)

window_width = min(window_width, 3000)
window_height = min(window_height, 2000)
window = pygame.display.set_mode((window_width, window_height), pygame.RESIZABLE)
pygame.display.set_caption('Tablets')

def draw_tablet(tablet, x, y):
    tablet.x = x
    tablet.y = y
    w = tablet.size_frac * tablet_w
    h = tablet.size_frac * (tablet_h + 2 * tablet_frame_size)
    if h > 2 * tablet_frame_size:
        color = tablet_colors[tablet.state]
        if show_table_tag:
            table_tag_color = tablet.table_tag_color
        else:
            table_tag_color = color

        pygame.draw.rect(window, table_tag_color, (x + tablet_frame_size + (tablet_w - w) / 2,
                                         y + tablet_frame_size,
                                         w,
                                         h - 2 * tablet_frame_size), border_radius=tablet_radius)

        if show_table_tag:
            table_tag_h = tablet_radius
            pygame.draw.rect(window, color, (x + tablet_frame_size + (tablet_w - w) / 2,
                                             y + tablet_frame_size,
                                             w,
                                             table_tag_h),
                             border_top_left_radius=tablet_radius,
                             border_top_right_radius=tablet_radius)

def draw_node_frame(x, y, x2, y2, color):
    pygame.draw.rect(window, color, (x, y, x2 - x, y2 - y), node_frame_thickness,
                     border_radius=tablet_radius + tablet_frame_size + node_frame_mid)

def animate(now):
    global changed

    tablet_max_h = tablet_h + 2 * tablet_frame_size

    def interpolate(start, duration):
        return min(1.0, (now - start) / duration)

    for node in nodes:
        for shard in node.shards:
            tablet_max_pos = 0
            for tablet in shard.ordered_tablets():
                # Appearing
                if tablet.h < tablet_max_h:
                    frac = cubic_ease_in(interpolate(tablet.insert_time, insert_time_ms))
                    new_h = frac * tablet_max_h
                    tablet.pos += new_h - tablet.h
                    tablet.h = new_h
                    tablet.size_frac = frac
                    changed = True

                # Falling
                if tablet.pos > tablet_max_pos + tablet.h:
                    if not tablet.fall_start:
                        tablet.fall_start = now
                        tablet.speed = 0
                        tablet.fall_real_start = now + fall_delay_ms
                        tablet.last_now = tablet.fall_real_start

                    if now > tablet.fall_real_start:
                        dt = (now - tablet.last_now) / 1000 # Convert to seconds
                        tablet.pos -= tablet.speed * dt + fall_acceleration * dt * dt / 2
                        tablet.speed += fall_acceleration * dt
                        tablet.last_now = now
                        changed = True

                # Bouncing
                if tablet.pos < tablet_max_pos + tablet.h:
                    tablet.pos = tablet_max_pos + tablet.h
                    tablet.fall_start = None
                    changed = True

                tablet_max_pos = tablet.pos


def redraw():
    """
    Calculates final layout of objects and draws them
    """

    background_color = WHITE
    window.fill(background_color)

    node_x = frame_size
    node_y = frame_size

    for node in nodes:
        bottom_line = window_height - frame_size

        draw_node_frame(node_x + node_frame_mid,
                        node_y + node_frame_mid,
                        node_x + 2 * node_frame_size + len(node.shards) * (tablet_w + tablet_frame_size * 2) - node_frame_mid,
                        bottom_line - node_frame_mid,
                        GRAY)

        node_x += node_frame_size

        for shard in node.shards:
            for tablet in shard.ordered_tablets():
                tablet_y = bottom_line - node_frame_size - tablet.pos
                draw_tablet(tablet, node_x, tablet_y)

            node_x += tablet_frame_size * 2 + tablet_w

        node_x += node_frame_size

redraw()

last_update = pygame.time.get_ticks()
hour_glass = HourGlass(pygame.time.get_ticks())

running = True
while running:
    for event in pygame.event.get():
        if event.type == pygame.QUIT:
            running = False
        elif event.type == pygame.VIDEORESIZE:
            window_width, window_height = event.w, event.h
            changed = True
        elif event.type == pygame.KEYDOWN:
            if event.key == pygame.K_t:
                show_table_tag = not show_table_tag
                changed = True

    now = float(pygame.time.get_ticks())

    animate(now)

    if changed:
        changed = False
        redraw()

    now = pygame.time.get_ticks()
    move_tracers(now)
    draw_trace_lines(trace_lines, now)

    # Check connectivity with data source
    # and display a pane while reconnecting
    if data_source_alive:
        last_update = now
        data_source_alive = False
    elif now - last_update > cql_update_timeout_ms:
        surf = pygame.Surface(window.get_size())
        surf.fill(WHITE)
        surf.set_alpha(200)
        window.blit(surf, (0, 0))
        hour_glass.draw(now)
        changed = True

    pygame.display.flip()
    clock.tick(100)

pygame.quit()
