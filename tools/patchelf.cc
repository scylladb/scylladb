// Copyright (C) 2025-present ScyllaDB
// SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0

#include <iostream>
#include <fstream>
#include <vector>
#include <string>
#include <cstring>
#include <memory>
#include <stdexcept>
#include <cstdio>
#include <sys/stat.h>
#include <getopt.h>
#include <elf.h>
#include <bit>
#include <concepts>

class elf_patcher {
private:
    std::vector<uint8_t> _data;
    bool _is_little_endian;
    Elf64_Ehdr* _ehdr;
    Elf64_Phdr* _phdr_table;
    Elf64_Shdr* _shdr_table;
    std::string _string_table;
    std::string _input_filename;

    template <std::integral T>
    T read_value(const uint8_t* ptr) const {
        T val = *reinterpret_cast<const T*>(ptr);
        // Byte swap if host endianness differs from object endianness
        bool need_swap = (std::endian::native == std::endian::little) != _is_little_endian;
        return need_swap ? std::byteswap(val) : val;
    }

    template <std::integral T>
    void write_value(uint8_t* ptr, T val) {
        // Byte swap if host endianness differs from object endianness
        bool need_swap = (std::endian::native == std::endian::little) != _is_little_endian;
        *reinterpret_cast<T*>(ptr) = need_swap ? std::byteswap(val) : val;
    }

    void parse_elf_header() {
        if (_data.size() < sizeof(Elf64_Ehdr)) {
            throw std::runtime_error("File too small to be a valid ELF");
        }

        // Check ELF magic
        if (std::memcmp(_data.data(), ELFMAG, SELFMAG) != 0) {
            throw std::runtime_error("Not a valid ELF file");
        }

        _ehdr = reinterpret_cast<Elf64_Ehdr*>(_data.data());

        // Check for 64-bit
        if (_ehdr->e_ident[EI_CLASS] != ELFCLASS64) {
            throw std::runtime_error("Only 64-bit ELF files are supported");
        }

        _is_little_endian = (_ehdr->e_ident[EI_DATA] == ELFDATA2LSB);

        // Parse program header table
        uint64_t phoff = read_value<uint64_t>(reinterpret_cast<uint8_t*>(&_ehdr->e_phoff));
        uint16_t phnum = read_value<uint16_t>(reinterpret_cast<uint8_t*>(&_ehdr->e_phnum));

        if (phoff + phnum * sizeof(Elf64_Phdr) > _data.size()) {
            throw std::runtime_error("Invalid program header table");
        }

        _phdr_table = reinterpret_cast<Elf64_Phdr*>(_data.data() + phoff);

        // Parse section header table
        uint64_t shoff = read_value<uint64_t>(reinterpret_cast<uint8_t*>(&_ehdr->e_shoff));
        uint16_t shnum = read_value<uint16_t>(reinterpret_cast<uint8_t*>(&_ehdr->e_shnum));

        if (shoff != 0 && shnum > 0) {
            if (shoff + shnum * sizeof(Elf64_Shdr) > _data.size()) {
                throw std::runtime_error("Invalid section header table");
            }
            _shdr_table = reinterpret_cast<Elf64_Shdr*>(_data.data() + shoff);

            // Load string table for section names
            uint16_t shstrndx = read_value<uint16_t>(reinterpret_cast<uint8_t*>(&_ehdr->e_shstrndx));
            if (shstrndx < shnum) {
                Elf64_Shdr* strtab_shdr = &_shdr_table[shstrndx];
                uint64_t strtab_offset = read_value<uint64_t>(reinterpret_cast<uint8_t*>(&strtab_shdr->sh_offset));
                uint64_t strtab_size = read_value<uint64_t>(reinterpret_cast<uint8_t*>(&strtab_shdr->sh_size));

                if (strtab_offset + strtab_size <= _data.size()) {
                    _string_table = std::string(reinterpret_cast<const char*>(_data.data() + strtab_offset), strtab_size);
                }
            }
        } else {
            _shdr_table = nullptr;
        }
    }

    Elf64_Shdr* find_interp_section() {
        if (!_shdr_table) {
            return nullptr;
        }

        uint16_t shnum = read_value<uint16_t>(reinterpret_cast<uint8_t*>(&_ehdr->e_shnum));

        for (int i = 0; i < shnum; i++) {
            Elf64_Shdr* shdr = &_shdr_table[i];
            uint32_t sh_name = read_value<uint32_t>(reinterpret_cast<uint8_t*>(&shdr->sh_name));

            if (sh_name < _string_table.size()) {
                const char* name = _string_table.c_str() + sh_name;
                if (std::strcmp(name, ".interp") == 0) {
                    return shdr;
                }
            }
        }
        return nullptr;
    }

    Elf64_Phdr* find_interp_segment() {
        uint16_t phnum = read_value<uint16_t>(reinterpret_cast<uint8_t*>(&_ehdr->e_phnum));

        for (int i = 0; i < phnum; i++) {
            Elf64_Phdr* phdr = &_phdr_table[i];
            uint32_t p_type = read_value<uint32_t>(reinterpret_cast<uint8_t*>(&phdr->p_type));

            if (p_type == PT_INTERP) {
                return phdr;
            }
        }
        return nullptr;
    }

public:
    explicit elf_patcher(const std::string& filename) : _input_filename(filename) {
        std::ifstream file(filename, std::ios::binary);
        if (!file) {
            throw std::runtime_error("Cannot open file: " + filename);
        }

        // Get file size
        file.seekg(0, std::ios::end);
        size_t size = file.tellg();
        file.seekg(0, std::ios::beg);

        // Read entire file
        _data.resize(size);
        file.read(reinterpret_cast<char*>(_data.data()), size);

        if (!file) {
            throw std::runtime_error("Error reading file: " + filename);
        }

        parse_elf_header();
    }

    std::string get_current_interpreter() {
        // Try to get interpreter from .interp section first
        Elf64_Shdr* shdr = find_interp_section();
        if (shdr) {
            uint64_t sh_offset = read_value<uint64_t>(reinterpret_cast<uint8_t*>(&shdr->sh_offset));
            uint64_t sh_size = read_value<uint64_t>(reinterpret_cast<uint8_t*>(&shdr->sh_size));

            if (sh_offset + sh_size <= _data.size()) {
                const char* interp_start = reinterpret_cast<const char*>(_data.data() + sh_offset);

                // Find null terminator or use full size
                size_t len = 0;
                while (len < sh_size && interp_start[len] != '\0') {
                    len++;
                }

                return std::string(interp_start, len);
            }
        }

        // Fall back to PT_INTERP segment
        Elf64_Phdr* phdr = find_interp_segment();
        if (!phdr) {
            return "";
        }

        uint64_t p_offset = read_value<uint64_t>(reinterpret_cast<uint8_t*>(&phdr->p_offset));
        uint64_t p_filesz = read_value<uint64_t>(reinterpret_cast<uint8_t*>(&phdr->p_filesz));

        if (p_offset + p_filesz > _data.size()) {
            throw std::runtime_error("Invalid interpreter segment");
        }

        const char* interp_start = reinterpret_cast<const char*>(_data.data() + p_offset);

        // Find null terminator or use full size
        size_t len = 0;
        while (len < p_filesz && interp_start[len] != '\0') {
            len++;
        }

        return std::string(interp_start, len);
    }

    void set_interpreter(const std::string& new_interp) {
        std::string new_interp_with_null = new_interp + '\0';
        size_t new_interp_len = new_interp_with_null.size();

        // Append new interpreter string to end of file
        size_t new_offset = _data.size();
        _data.resize(_data.size() + new_interp_len);
        std::memcpy(_data.data() + new_offset, new_interp_with_null.c_str(), new_interp_len);

        // Update pointers after potential reallocation
        _ehdr = reinterpret_cast<Elf64_Ehdr*>(_data.data());
        uint64_t phoff = read_value<uint64_t>(reinterpret_cast<uint8_t*>(&_ehdr->e_phoff));
        _phdr_table = reinterpret_cast<Elf64_Phdr*>(_data.data() + phoff);

        uint64_t shoff = read_value<uint64_t>(reinterpret_cast<uint8_t*>(&_ehdr->e_shoff));
        if (shoff != 0) {
            _shdr_table = reinterpret_cast<Elf64_Shdr*>(_data.data() + shoff);
        }

        // Update PT_INTERP segment if it exists
        Elf64_Phdr* phdr = find_interp_segment();
        if (phdr) {
            write_value<uint64_t>(reinterpret_cast<uint8_t*>(&phdr->p_offset), new_offset);
            write_value<uint64_t>(reinterpret_cast<uint8_t*>(&phdr->p_filesz), new_interp_len);
            write_value<uint64_t>(reinterpret_cast<uint8_t*>(&phdr->p_memsz), new_interp_len);
        }

        // Update .interp section if it exists, or create one if section headers exist
        if (_shdr_table) {
            Elf64_Shdr* shdr = find_interp_section();
            if (shdr) {
                // Update existing .interp section
                write_value<uint64_t>(reinterpret_cast<uint8_t*>(&shdr->sh_offset), new_offset);
                write_value<uint64_t>(reinterpret_cast<uint8_t*>(&shdr->sh_size), new_interp_len);
            } else {
                // Create new .interp section
                create_interp_section(new_offset, new_interp_len);
            }
        }
    }

private:
    void create_interp_section(uint64_t offset, uint64_t size) {
        // We need to expand the section header table and string table
        uint16_t shnum = read_value<uint16_t>(reinterpret_cast<uint8_t*>(&_ehdr->e_shnum));
        uint64_t shoff = read_value<uint64_t>(reinterpret_cast<uint8_t*>(&_ehdr->e_shoff));

        if (shoff == 0 || shnum == 0) {
            // No section headers exist, can't create sections
            return;
        }

        // Add ".interp" to string table
        std::string new_name = ".interp";
        size_t name_offset = _string_table.size();
        _string_table += new_name + '\0';

        // Update string table in file
        uint16_t shstrndx = read_value<uint16_t>(reinterpret_cast<uint8_t*>(&_ehdr->e_shstrndx));
        if (shstrndx < shnum) {
            Elf64_Shdr* strtab_shdr = &_shdr_table[shstrndx];
            uint64_t strtab_offset = read_value<uint64_t>(reinterpret_cast<uint8_t*>(&strtab_shdr->sh_offset));

            // Resize data to accommodate new string table
            size_t old_strtab_size = read_value<uint64_t>(reinterpret_cast<uint8_t*>(&strtab_shdr->sh_size));
            size_t new_strtab_size = _string_table.size();

            // Move section header table to end if it's after the string table
            if (shoff > strtab_offset + old_strtab_size) {
                size_t new_shoff = _data.size();
                _data.resize(_data.size() + (shnum + 1) * sizeof(Elf64_Shdr));
                std::memmove(_data.data() + new_shoff, _data.data() + shoff, shnum * sizeof(Elf64_Shdr));

                // Update section header table offset
                write_value<uint64_t>(reinterpret_cast<uint8_t*>(&_ehdr->e_shoff), new_shoff);
                _shdr_table = reinterpret_cast<Elf64_Shdr*>(_data.data() + new_shoff);
                strtab_shdr = &_shdr_table[shstrndx];
            } else {
                // Expand data for new section header
                _data.resize(_data.size() + sizeof(Elf64_Shdr));
                // Refresh pointers after reallocation
                _ehdr = reinterpret_cast<Elf64_Ehdr*>(_data.data());
                shoff = read_value<uint64_t>(reinterpret_cast<uint8_t*>(&_ehdr->e_shoff));
                _shdr_table = reinterpret_cast<Elf64_Shdr*>(_data.data() + shoff);
                strtab_shdr = &_shdr_table[shstrndx];
            }

            // Update string table size and content
            write_value<uint64_t>(reinterpret_cast<uint8_t*>(&strtab_shdr->sh_size), new_strtab_size);
            std::memcpy(_data.data() + strtab_offset, _string_table.c_str(), new_strtab_size);
        }

        // Create new .interp section header
        Elf64_Shdr new_section = {};
        write_value<uint32_t>(reinterpret_cast<uint8_t*>(&new_section.sh_name), name_offset);
        write_value<uint32_t>(reinterpret_cast<uint8_t*>(&new_section.sh_type), SHT_PROGBITS);
        write_value<uint64_t>(reinterpret_cast<uint8_t*>(&new_section.sh_flags), SHF_ALLOC);
        write_value<uint64_t>(reinterpret_cast<uint8_t*>(&new_section.sh_addr), 0);
        write_value<uint64_t>(reinterpret_cast<uint8_t*>(&new_section.sh_offset), offset);
        write_value<uint64_t>(reinterpret_cast<uint8_t*>(&new_section.sh_size), size);
        write_value<uint32_t>(reinterpret_cast<uint8_t*>(&new_section.sh_link), 0);
        write_value<uint32_t>(reinterpret_cast<uint8_t*>(&new_section.sh_info), 0);
        write_value<uint64_t>(reinterpret_cast<uint8_t*>(&new_section.sh_addralign), 1);
        write_value<uint64_t>(reinterpret_cast<uint8_t*>(&new_section.sh_entsize), 0);

        // Add section header to table
        std::memcpy(_data.data() + shoff + shnum * sizeof(Elf64_Shdr), &new_section, sizeof(Elf64_Shdr));

        // Update section count
        write_value<uint16_t>(reinterpret_cast<uint8_t*>(&_ehdr->e_shnum), shnum + 1);
    }

public:

    void save(const std::string& filename) {
        // Get original file permissions from input file
        struct stat file_stat;
        if (stat(_input_filename.c_str(), &file_stat) != 0) {
            throw std::runtime_error("Cannot get file permissions for: " + _input_filename);
        }

        std::string temp_filename = filename + ".tmp";

        std::ofstream file(temp_filename, std::ios::binary);
        if (!file) {
            throw std::runtime_error("Cannot create output file: " + temp_filename);
        }

        file.write(reinterpret_cast<const char*>(_data.data()), _data.size());
        if (!file) {
            throw std::runtime_error("Error writing to file: " + temp_filename);
        }

        file.close();

        // Set the same permissions on the temporary file
        if (chmod(temp_filename.c_str(), file_stat.st_mode) != 0) {
            std::remove(temp_filename.c_str());
            throw std::runtime_error("Cannot set permissions on temp file: " + temp_filename);
        }

        // Atomically replace the original file
        if (std::rename(temp_filename.c_str(), filename.c_str()) != 0) {
            // Clean up temp file on failure
            std::remove(temp_filename.c_str());
            throw std::runtime_error("Cannot replace original file: " + filename);
        }
    }
};

void usage(const char* progname) {
    std::cerr << "Usage: " << progname << " [OPTIONS] FILE\n";
    std::cerr << "Options:\n";
    std::cerr << "  --set-interpreter INTERPRETER  Set the ELF interpreter\n";
    std::cerr << "  --print-interpreter            Print the current ELF interpreter\n";
    std::cerr << "  --output FILE                  Output file (default: modify in place)\n";
    std::cerr << "  --help                         Show this help\n";
}

int main(int argc, char* argv[]) {
    static struct option long_options[] = {
        {"set-interpreter", required_argument, 0, 's'},
        {"print-interpreter", no_argument, 0, 'p'},
        {"output", required_argument, 0, 'o'},
        {"help", no_argument, 0, 'h'},
        {0, 0, 0, 0}
    };

    std::string set_interp;
    std::string output_file;
    bool print_interp = false;
    int c;

    while ((c = getopt_long(argc, argv, "s:po:h", long_options, nullptr)) != -1) {
        switch (c) {
        case 's':
            set_interp = optarg;
            break;
        case 'p':
            print_interp = true;
            break;
        case 'o':
            output_file = optarg;
            break;
        case 'h':
            usage(argv[0]);
            return 0;
        default:
            usage(argv[0]);
            return 1;
        }
    }

    if (optind >= argc) {
        std::cerr << "Missing input file\n";
        usage(argv[0]);
        return 1;
    }

    if (set_interp.empty() && !print_interp) {
        std::cerr << "Must specify either --set-interpreter or --print-interpreter\n";
        usage(argv[0]);
        return 1;
    }

    std::string input_file = argv[optind];

    try {
        elf_patcher patcher(input_file);

        if (print_interp) {
            std::string interp = patcher.get_current_interpreter();
            if (interp.empty()) {
                std::cerr << "No interpreter found\n";
                return 1;
            }
            std::cout << interp << '\n';
        }

        if (!set_interp.empty()) {
            patcher.set_interpreter(set_interp);

            std::string save_filename = output_file.empty() ? input_file : output_file;
            patcher.save(save_filename);

            std::cout << "Set interpreter to: " << set_interp << '\n';
        }

    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << '\n';
        return 1;
    }

    return 0;
}
