/*
 * Copyright (C) 2020 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * Scylla is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Scylla is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Scylla.  If not, see <http://www.gnu.org/licenses/>.
 */


#define _USE_MATH_DEFINES
#include <cmath>
#include <random>
#include <seastar/core/sstring.hh>
#include "redis/lolwut.hh"
#include "redis/version.hh"

using namespace seastar;

namespace redis {

class canvas {
    const int _width, _height;
    std::unique_ptr<char[]> _pixels;
public:
    canvas(const int width, int height) : _width(width), _height(height), _pixels(std::make_unique<char[]>(width * height)) {}

    void draw_pixel(const int x, const int y, const int color) {
        if (x < 0 || x >= _width || y < 0 || y >= _height) {
            return;
        }
        _pixels[x + y * _width] = color;
    }

    static void translate_pixels_group(const int byte, int8_t *output) {
        const int code = 0x2800 + byte;
        output[0] = 0xe0 | (code >> 12);
        output[1] = 0x80 | ((code >> 6) & 0x3f);
        output[2] = 0x80 | (code & 0x3f);
    }

    int get_pixel(const int x, const int y) {
        if (x < 0 || x >= _width || y < 0 || y >= _height) {
            return 0;
        }
        return _pixels[x + y * _width];
    }

    void draw_line(int x1, int y1, const int x2, const int y2, const int color) {
        const int dx = abs(x2 - x1);
        const int dy = abs(y2 - y1);
        const int sx = (x1 < x2) ? 1 : -1;
        const int sy = (y1 < y2) ? 1 : -1;
        int err = dx - dy;
        int e2;

        while (1) {
            draw_pixel(x1, y1, color);
            if (x1 == x2 && y1 == y2) {
                break;
            }
            e2 = err * 2;
            if (e2 > -dy) {
                err -= dy;
                x1 += sx;
            }
            if (e2 < dx) {
                err += dx;
                y1 += sy;
            }
        }
    }

    void draw_square(const int x, const int y, float size, const float angle) {
        int px[4], py[4];

        size /= 1.4142135623;
        size = round(size);

        float k = M_PI/4 + angle;
        for (int j = 0; j < 4; j++) {
            px[j] = round(sin(k) * size + x);
            py[j] = round(cos(k) * size + y);
            k += M_PI/2;
        }

        for (int j = 0; j < 4; j++) {
            draw_line(px[j], py[j], px[(j + 1) % 4], py[(j + 1) % 4], 1);
        }
    }

    static canvas draw_schotter(const int cols, const int squares_per_row, const int squares_per_col) {
        const int width = cols * 2;
        const int padding = width > 4 ? 2 : 0;
        const float side = static_cast<float>((width - padding * 1) / squares_per_row);
        const int height = side * squares_per_col + padding * 0;
        canvas c {width, height};
        std::random_device seed_gen;
        std::default_random_engine engine(seed_gen());
        std::uniform_real_distribution<float> dist(0.0, 1.0);

        for (int y = 0; y < squares_per_col; y++) {
            for (int x = 0; x < squares_per_row; x++) {
                int sx = x * side + side / 2 + padding;
                int sy = y * side + side / 2 + padding;

                float angle = 0;
                if (y > 1) {
                    float r1 = dist(engine) / squares_per_col * y;
                    float r2 = dist(engine) / squares_per_col * y;
                    float r3 = dist(engine) / squares_per_col * y;
                    if (engine() % 2) {
                        r1 = -r1;
                    }
                    if (engine() % 2) {
                        r2 = -r2;
                    }
                    if (engine() % 2) {
                        r3 = -r3;
                    }
                    angle = r1;
                    sx += r2 * side / 3;
                    sy += r3 * side / 3;
                }
                c.draw_square(sx, sy, side, angle);
            }
        }
        return c;
    }

    bytes render_canvas() {
        // txt size = height * width * pixels 3byte + height * return code 1byte
        bytes txt(bytes::initialized_later(), (_height / 4) * (_width / 2) * 3 + (_height / 4) * 1);
        int i = 0;
        for (int y = 0; y < _height; y += 4) {
            for (int x = 0; x < _width; x += 2) {
                int byte = 0;
                int8_t unicode[3];
                if (get_pixel(x, y)) {
                    byte |= (1 << 0);
                }
                if (get_pixel(x, y + 1)) {
                    byte |= (1 << 1);
                }
                if (get_pixel(x, y + 2)) {
                    byte |= (1 << 2);
                }
                if (get_pixel(x + 1, y)) {
                    byte |= (1 << 3);
                }
                if (get_pixel(x + 1, y + 1)) {
                    byte |= (1 << 4);
                }
                if (get_pixel(x + 1, y + 2)) {
                    byte |= (1 << 5);
                }
                if (get_pixel(x, y + 3)) {
                    byte |= (1 << 6);
                }
                if (get_pixel(x + 1, y + 3)) {
                    byte |= (1 << 7);
                }
                translate_pixels_group(byte, unicode);
                std::memcpy(&txt[i], &unicode[0], 3);
                i += 3;
            }
            if (y != _height - 1) {
                txt[i++] = '\n';
            }
        }
        return txt;
    }

};

future<bytes> lolwut5(const int cols, const int squares_per_row, const int squares_per_col) {
    auto c = canvas::draw_schotter(cols, squares_per_row, squares_per_col);
    auto rendered = c.render_canvas();
    const bytes msg {"\nGeorg Nees - schotter, plotter on paper, 1968. Redis ver. "};
    const bytes nr {"\n"};
    rendered += msg;
    rendered += redis_version;
    rendered += nr;
    return make_ready_future<bytes>(rendered);
}

}