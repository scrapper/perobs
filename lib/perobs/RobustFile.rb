# encoding: UTF-8
#
# = RobustFile.rb -- Persistent Ruby Object Store
#
# Copyright (c) 2015, 2016 by Chris Schlaeger <chris@taskjuggler.org>
#
# MIT License
#
# Permission is hereby granted, free of charge, to any person obtaining
# a copy of this software and associated documentation files (the
# "Software"), to deal in the Software without restriction, including
# without limitation the rights to use, copy, modify, merge, publish,
# distribute, sublicense, and/or sell copies of the Software, and to
# permit persons to whom the Software is furnished to do so, subject to
# the following conditions:
#
# The above copyright notice and this permission notice shall be
# included in all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
# MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
# NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
# LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
# OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
# WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

require 'perobs/Log'

module PEROBS

  class RobustFile

    # This is a more robust version of File::write. It first writes to a
    # temporary file and them atomically renames the new file to the
    # potentially existing old file. This should significantly increase our
    # chances that we never end up with a corrupted file due to problems
    # during the file write.
    def RobustFile::write(name, string)
      tmp_name = name + '.atomic'

      # Ensure that no file with the temporary name exists.
      if File.exist?(tmp_name)
        PEROBS.log.warn "Found old temporary file #{tmp_name}"
        unless File.exist?(name)
          # If we have a file with the temporary name but no file with the
          # actual name, rename the temporary file to the actual name. We have
          # no idea if that temporary file is good enough, but it's better to
          # have a file with the actual name as backup in case this file write
          # fails.
          begin
            File.rename(tmp_name, name)
          rescue IOError => e
            raise IOError "Could not name old temporary file to #{name}: " +
              e.message
          end
          Log.warn "Found old temporary file but no corresponding original file"
        else
          # Delete the old temporary file.
          begin
            File.delete(tmp_name)
          rescue IOError => e
            PEROBS.log.warn "Could not delete old temporary file " +
              "#{tmp_name}: #{e.message}"
          end
        end
      end

      # Write the temporary file.
      begin
        File.write(tmp_name, string)
      rescue IOError => e
        raise IOError "Could not write file #{tmp_name}: #{e.message}"
      end

      # If the temporary file was written successfully we can rename it to the
      # actual file name and atomically replace the old file if it exists.
      begin
        File.rename(tmp_name, name)
      rescue IOError => e
         raise IOError "Could not rename #{tmp_name} to #{name}: #{e.message}"
      end
    end

  end

end

